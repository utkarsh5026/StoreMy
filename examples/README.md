# StoreMy Database - Example SQL Files

This directory contains sample SQL files for testing and demonstrating StoreMy Database capabilities.

## Available Examples

### 1. `sample_queries.sql` - Comprehensive Demo
**Best for:** Complete feature walkthrough

**What's included:**
- 3 tables: employees, departments, projects
- 20+ queries covering all major operations
- CRUD operations with examples
- JOIN operations (multi-table)
- Aggregations and filtering
- Complex queries

**Use with:**
```bash
docker-compose up storemy-import
# or
./storemy --import examples/sample_queries.sql
```

**Topics covered:**
- ✅ CREATE TABLE
- ✅ INSERT (multiple rows)
- ✅ SELECT (basic and filtered)
- ✅ UPDATE (with conditions)
- ✅ DELETE (with conditions)
- ✅ JOIN (inner joins)
- ✅ Aggregations (COUNT)
- ✅ Complex multi-table queries

### 2. `quick_test.sql` - Fast Validation
**Best for:** Quick "does it work?" check

**What's included:**
- 1 simple table: users
- Basic CRUD operations
- ~15 lines total

**Use with:**
```bash
./storemy --import examples/quick_test.sql
```

**Topics covered:**
- ✅ CREATE TABLE
- ✅ INSERT
- ✅ SELECT with filter
- ✅ COUNT
- ✅ UPDATE
- ✅ DELETE

## Usage Examples

### Method 1: Docker Compose (Recommended)
```bash
# Use sample_queries.sql
docker-compose up storemy-import

# The database will start with all examples pre-loaded
# Then you can interact with it via the UI
```

### Method 2: Command Line Flag
```bash
# Build first
go build -o storemy

# Import any SQL file
./storemy --import examples/sample_queries.sql
./storemy --import examples/quick_test.sql
```

### Method 3: Copy-Paste in UI
```bash
# Start the database
./storemy --demo

# Open quick_test.sql in a text editor
# Copy queries one by one and paste into UI
# Press Ctrl+E to execute each query
```

## Creating Your Own Examples

### Template Structure
```sql
-- Your Example Name
-- Description of what this demonstrates

-- 1. Create tables
CREATE TABLE your_table (
    id INT,
    name VARCHAR,
    value INT
);

-- 2. Insert data
INSERT INTO your_table VALUES (1, 'test', 100);

-- 3. Query data
SELECT * FROM your_table;

-- 4. Modify data
UPDATE your_table SET value = 200 WHERE your_table.id = 1;

-- 5. Delete data
DELETE FROM your_table WHERE your_table.id = 1;
```

### Important Syntax Notes

1. **Use fully qualified names in WHERE clauses:**
   ```sql
   ✅ SELECT * FROM users WHERE users.age > 25;
   ❌ SELECT * FROM users WHERE age > 25;
   ```

2. **COUNT requires field name:**
   ```sql
   ✅ SELECT COUNT(users.id) FROM users;
   ❌ SELECT COUNT(*) FROM users;
   ```

3. **Supported data types:**
   - `INT` - Integer values
   - `VARCHAR` - Variable-length strings
   - `FLOAT` - Floating-point numbers
   - `BOOL` - Boolean values

## Query Cookbook

### Basic Operations

**Create a table:**
```sql
CREATE TABLE products (id INT, name VARCHAR, price INT);
```

**Insert data:**
```sql
INSERT INTO products VALUES (1, 'Laptop', 1200);
INSERT INTO products VALUES (2, 'Mouse', 25);
```

**Select all:**
```sql
SELECT * FROM products;
```

**Select with filter:**
```sql
SELECT * FROM products WHERE products.price > 100;
```

**Update:**
```sql
UPDATE products SET price = 1300 WHERE products.id = 1;
```

**Delete:**
```sql
DELETE FROM products WHERE products.id = 2;
```

### Advanced Operations

**Join two tables:**
```sql
SELECT * FROM orders
JOIN customers ON orders.customer_id = customers.id;
```

**Count rows:**
```sql
SELECT COUNT(products.id) FROM products;
```

**Count with filter:**
```sql
SELECT COUNT(products.id) FROM products
WHERE products.price > 100;
```

**Order results:**
```sql
SELECT * FROM products ORDER BY products.price DESC;
```

**Limit results:**
```sql
SELECT * FROM products LIMIT 10;
```

## Example Datasets

### E-Commerce
```sql
CREATE TABLE customers (id INT, name VARCHAR, email VARCHAR);
CREATE TABLE orders (id INT, customer_id INT, total INT);
CREATE TABLE products (id INT, name VARCHAR, price INT);

INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com');
INSERT INTO orders VALUES (101, 1, 150);
INSERT INTO products VALUES (1, 'Widget', 50);
```

### Blog System
```sql
CREATE TABLE users (id INT, username VARCHAR, email VARCHAR);
CREATE TABLE posts (id INT, user_id INT, title VARCHAR, likes INT);
CREATE TABLE comments (id INT, post_id INT, user_id INT, text VARCHAR);
```

### Inventory Management
```sql
CREATE TABLE items (id INT, name VARCHAR, quantity INT, location VARCHAR);
CREATE TABLE suppliers (id INT, name VARCHAR, contact VARCHAR);
CREATE TABLE orders (id INT, item_id INT, supplier_id INT, quantity INT);
```

## Testing Your Examples

### Manual Testing
1. Start database: `./storemy --demo`
2. Copy your SQL
3. Paste into UI
4. Press Ctrl+E to execute
5. Verify results

### Automated Testing
1. Save your SQL to `examples/my_test.sql`
2. Run: `./storemy --import examples/my_test.sql`
3. Check for errors in output

## Tips for Writing Examples

1. **Start simple** - One table, basic operations
2. **Add complexity gradually** - Joins, filters, aggregations
3. **Comment liberally** - Explain what each section does
4. **Test before sharing** - Verify all queries work
5. **Use realistic data** - Makes examples more relatable

## Common Patterns

### CRUD Pattern
```sql
-- Create
CREATE TABLE items (id INT, name VARCHAR);

-- Read
SELECT * FROM items;

-- Update
UPDATE items SET name = 'new_name' WHERE items.id = 1;

-- Delete
DELETE FROM items WHERE items.id = 1;
```

### Master-Detail Pattern
```sql
-- Master table
CREATE TABLE orders (id INT, customer VARCHAR, total INT);

-- Detail table
CREATE TABLE order_items (id INT, order_id INT, product VARCHAR, qty INT);

-- Query with join
SELECT * FROM orders
JOIN order_items ON orders.id = order_items.order_id;
```

### Aggregation Pattern
```sql
-- Count by category
SELECT department, COUNT(employees.id)
FROM employees
GROUP BY employees.department;
```

## Need Help?

- **Syntax errors?** Check [DOCKER_README.md](../DOCKER_README.md) for SQL syntax guide
- **Want to see what's possible?** Read [END_TO_END_ASSESSMENT.md](../END_TO_END_ASSESSMENT.md)
- **Planning complex queries?** See [sample_queries.sql](sample_queries.sql) for examples

## Contributing Examples

Have a great example? Consider adding it:
1. Create your SQL file
2. Test thoroughly
3. Add documentation comments
4. Update this README
5. Submit a pull request

---

**Happy Querying!** 🎯
