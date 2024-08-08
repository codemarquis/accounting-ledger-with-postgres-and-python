import psycopg

DB_NAME = 'ledger'
DB_USER = 'ledger'

class Database:
    def __init__(self):
        self.db_connection = psycopg.connect(dbname=DB_NAME, user=DB_USER)
        self.cur = self.db_connection.cursor()

    def setup_db_schema(self):
        with open('schema.sql', 'r') as f:
            self.cur.execute(f.read())
        self.db_connection.commit()

    def drop_db_schema(self):
        self.cur.execute('DROP SCHEMA public CASCADE;')
        self.cur.execute('CREATE SCHEMA public;')
        self.db_connection.commit()



class Ledger:
    def __init__(self):
        self.db = Database()

    def add_account(self, name, number):
        with self.db.db_connection.transaction():
            res = self.db.cur.execute('INSERT INTO accounts (name, number) VALUES (%s, %s) RETURNING id;', (name, number))
        return res.fetchone()[0]

    def add_journal(self, journal, journal_lines):
        # we need to run the insert statements in a transaction for the triggers to work which ensure the journals balance
        with self.db.db_connection.transaction():
            res = self.db.cur.execute('INSERT INTO journals (date, narration) VALUES (%s, %s) RETURNING id;', (journal["date"], journal["narration"], ))
            journal_id = res.fetchone()[0]
            res = self.db.cur.executemany('INSERT INTO journal_lines (journal_id, type, amount, account_id) VALUES (%s, %s, %s, %s);', [(journal_id, line["type"], line["amount"], line["account_id"]) for line in journal_lines])
        return res

    def get_trial_balance(self):
        res = self.db.cur.execute("""
          SELECT
                a.number AS account_number,
                a.name AS account_name,
                COALESCE(SUM(CASE WHEN jl.type = 'debit' THEN jl.amount ELSE 0 END), 0) AS debit,
                COALESCE(SUM(CASE WHEN jl.type = 'credit' THEN jl.amount ELSE 0 END), 0) AS credit
            FROM 
                accounts a
            LEFT JOIN 
                journal_lines jl ON a.id = jl.account_id
            GROUP BY 
                a.id
            ORDER BY 
                a.number;
        """)
        return res.fetchall()

ledger = Ledger()
ledger.db.drop_db_schema()
ledger.db.setup_db_schema()

account_id = ledger.add_account("Revenues", 100)
expenses_account_id = ledger.add_account("Expenses", 200)
ledger.add_journal({
    "id": 1,
    "date": "2020-01-01",
    "narration": "Sale of goods"
    },
    [
        {
            "type": "debit",
            "amount": 100,
            "account_id": account_id
        },
        {
            "type": "credit",
            "amount": 100,
            "account_id": account_id
        }
    ])

# uncomment this for a failing test:
#ledger.add_journal({
#    "id": 1,
#    "date": "2020-01-01"
#    },
#    [
#        {
#            "type": "debit",
#            "description": "Opening balance",
#            "amount": 101,
#            "account_id": cash_account_id
#        },
#        {
#            "type": "credit",
#            "description": "Opening balance",
#            "amount": 100,
#            "account_id": expenses_account_id
#        }
#    ])
print("Trial balance:")
print(ledger.get_trial_balance())
