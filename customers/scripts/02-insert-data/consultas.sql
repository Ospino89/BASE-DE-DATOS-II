CREATE OR REPLACE FUNCTION update_price_cop_af_ins()
RETURN TRIGGER
LANGUAGE plpgsql
AS 
$$
    BEGIN

    UPDATE cs.products
    SET cop_price = (usd_price * 3750)
    WHERE id = NEW.id;

    RETURN NEW;

END;
$$;


CREATE TRIGGER tgg_update_price_cop_af_ins
AFTER INSERT ON cs.products
FOR EACH ROW
EXECUTE FUNCTION update_price_cop_af_ins();


SELECT 
COUNT(*)
FROM cs.products
WHERE cop_price IS NULL;








CREATE OR REPLACE FUNCTION update_category_id(product_id INT, category_id INT)
RETURN TEXT
LANGUAGE plpgsql
AS 
$$
    DECLARE
    rows_affected INTEGER;
    BEGIN
    UPDATE cs.products
    SET category_id = category_id
    WHERE id = product_id;

    RETURN NEW;

END;
$$;




UPDATE cs.products
SET CATEGORY








