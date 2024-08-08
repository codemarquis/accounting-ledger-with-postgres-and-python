CREATE TABLE journals (
	id SERIAL PRIMARY KEY,
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date DATE NOT NULL,
    narration TEXT NOT NULL
);

CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    name TEXT NOT NULL,
    number TEXT NOT NULL
);

CREATE TYPE JournalType AS ENUM ('debit', 'credit');

CREATE TABLE journal_lines (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    journal_id INTEGER REFERENCES journals(id) NOT NULL,
    type JournalType NOT NULL,
    amount FLOAT NOT NULL,
    account_id INTEGER REFERENCES accounts(id) NOT NULL
    CONSTRAINT journal_lines_amount CHECK (amount > 0)
);

-- triggers

-- Ensure that journal lines belonging to a journal balance when a journal gets inserted or updated
CREATE OR REPLACE FUNCTION check_journals_balance(journal_id INTEGER) 
RETURNS BOOLEAN AS $$
DECLARE
    debit_total FLOAT;
    credit_total FLOAT;
    line_count INTEGER;
BEGIN
    SELECT 
        COALESCE(SUM(CASE WHEN type = 'debit' THEN amount ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN type = 'credit' THEN amount ELSE 0 END), 0),
        COUNT(*)
    INTO debit_total, credit_total, line_count
    FROM journal_lines
    WHERE journal_lines.journal_id = $1;
    RETURN debit_total = credit_total AND debit_total > 0 AND line_count >= 2;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION enforce_journal_balance() 
RETURNS TRIGGER AS $$
DECLARE
    is_balanced BOOLEAN;
BEGIN
    -- If it's a delete operation, use OLD.journal_id, otherwise use NEW.journal_id
    is_balanced := check_journals_balance(COALESCE(NEW.journal_id, OLD.journal_id));
    
    IF NOT is_balanced THEN
        RAISE EXCEPTION 'Journal is not balanced or does not have at least two lines';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER enforce_journal_balance
AFTER INSERT OR UPDATE OR DELETE ON journal_lines
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION enforce_journal_balance();
