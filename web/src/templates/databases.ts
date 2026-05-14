export interface DatabaseTemplate {
  id: string;
  name: string;
  description: string;
  defaultDatabaseName: string;
  tables: string[];
  starterQuery: string;
  statements: string[];
}

export const DATABASE_TEMPLATES: DatabaseTemplate[] = [
  {
    id: "college",
    name: "College",
    description: "Students, courses, instructors, and enrollments.",
    defaultDatabaseName: "college",
    tables: ["students", "courses", "instructors", "enrollments"],
    starterQuery: "SELECT * FROM students;",
    statements: [
      "CREATE TABLE IF NOT EXISTS students (id INT, name VARCHAR, major VARCHAR, year INT)",
      "CREATE TABLE IF NOT EXISTS courses (id INT, title VARCHAR, credits INT, instructor_id INT)",
      "CREATE TABLE IF NOT EXISTS instructors (id INT, name VARCHAR, department VARCHAR)",
      "CREATE TABLE IF NOT EXISTS enrollments (student_id INT, course_id INT, grade VARCHAR)",
      "INSERT INTO students VALUES (1, 'Anika Rao', 'Computer Science', 2), (2, 'Miles Chen', 'Physics', 3), (3, 'Sara Khan', 'Economics', 1)",
      "INSERT INTO instructors VALUES (1, 'Dr Mehta', 'Computer Science'), (2, 'Dr Wilson', 'Physics')",
      "INSERT INTO courses VALUES (101, 'Database Systems', 4, 1), (102, 'Classical Mechanics', 3, 2), (103, 'Microeconomics', 3, 1)",
      "INSERT INTO enrollments VALUES (1, 101, 'A'), (2, 102, 'B'), (3, 103, 'A')",
    ],
  },
  {
    id: "library",
    name: "Library",
    description: "Books, members, authors, and loans.",
    defaultDatabaseName: "library",
    tables: ["books", "members", "authors", "loans"],
    starterQuery: "SELECT * FROM books;",
    statements: [
      "CREATE TABLE IF NOT EXISTS books (id INT, title VARCHAR, author_id INT, published_year INT)",
      "CREATE TABLE IF NOT EXISTS authors (id INT, name VARCHAR, country VARCHAR)",
      "CREATE TABLE IF NOT EXISTS members (id INT, name VARCHAR, joined_year INT)",
      "CREATE TABLE IF NOT EXISTS loans (book_id INT, member_id INT, loaned_on VARCHAR, returned VARCHAR)",
      "INSERT INTO authors VALUES (1, 'Ursula Le Guin', 'USA'), (2, 'R K Narayan', 'India'), (3, 'Mary Shelley', 'England')",
      "INSERT INTO books VALUES (1, 'A Wizard of Earthsea', 1, 1968), (2, 'Malgudi Days', 2, 1943), (3, 'Frankenstein', 3, 1818)",
      "INSERT INTO members VALUES (1, 'Nora Patel', 2024), (2, 'Owen Park', 2025)",
      "INSERT INTO loans VALUES (1, 1, '2026-05-01', 'no'), (3, 2, '2026-05-08', 'yes')",
    ],
  },
  {
    id: "store_inventory",
    name: "Store Inventory",
    description: "Products, suppliers, stock levels, and sales.",
    defaultDatabaseName: "store_inventory",
    tables: ["products", "suppliers", "stock", "sales"],
    starterQuery: "SELECT * FROM products;",
    statements: [
      "CREATE TABLE IF NOT EXISTS products (id INT, name VARCHAR, category VARCHAR, price INT)",
      "CREATE TABLE IF NOT EXISTS suppliers (id INT, name VARCHAR, city VARCHAR)",
      "CREATE TABLE IF NOT EXISTS stock (product_id INT, supplier_id INT, quantity INT)",
      "CREATE TABLE IF NOT EXISTS sales (id INT, product_id INT, quantity INT, sold_on VARCHAR)",
      "INSERT INTO products VALUES (1, 'Notebook', 'Stationery', 80), (2, 'Backpack', 'Bags', 1200), (3, 'Water Bottle', 'Accessories', 350)",
      "INSERT INTO suppliers VALUES (1, 'North Supply', 'Delhi'), (2, 'Campus Goods', 'Pune')",
      "INSERT INTO stock VALUES (1, 1, 120), (2, 2, 35), (3, 2, 60)",
      "INSERT INTO sales VALUES (1, 1, 5, '2026-05-10'), (2, 3, 2, '2026-05-11')",
    ],
  },
  {
    id: "clinic",
    name: "Clinic",
    description: "Patients, doctors, appointments, and prescriptions.",
    defaultDatabaseName: "clinic",
    tables: ["patients", "doctors", "appointments", "prescriptions"],
    starterQuery: "SELECT * FROM appointments;",
    statements: [
      "CREATE TABLE IF NOT EXISTS patients (id INT, name VARCHAR, age INT, city VARCHAR)",
      "CREATE TABLE IF NOT EXISTS doctors (id INT, name VARCHAR, specialty VARCHAR)",
      "CREATE TABLE IF NOT EXISTS appointments (id INT, patient_id INT, doctor_id INT, appointment_day VARCHAR)",
      "CREATE TABLE IF NOT EXISTS prescriptions (appointment_id INT, medicine VARCHAR, dosage VARCHAR)",
      "INSERT INTO patients VALUES (1, 'Isha Nair', 29, 'Bengaluru'), (2, 'Kabir Das', 41, 'Mumbai'), (3, 'Leah Roy', 34, 'Kolkata')",
      "INSERT INTO doctors VALUES (1, 'Dr Sen', 'Cardiology'), (2, 'Dr Kapoor', 'Dermatology')",
      "INSERT INTO appointments VALUES (1, 1, 1, '2026-05-15'), (2, 2, 2, '2026-05-16')",
      "INSERT INTO prescriptions VALUES (1, 'Atenolol', 'once daily'), (2, 'Cetirizine', 'night')",
    ],
  },
  {
    id: "project_tracker",
    name: "Project Tracker",
    description: "Projects, teammates, tasks, and milestones.",
    defaultDatabaseName: "project_tracker",
    tables: ["projects", "members", "tasks", "milestones"],
    starterQuery: "SELECT * FROM tasks;",
    statements: [
      "CREATE TABLE IF NOT EXISTS projects (id INT, name VARCHAR, status VARCHAR)",
      "CREATE TABLE IF NOT EXISTS members (id INT, name VARCHAR, role VARCHAR)",
      "CREATE TABLE IF NOT EXISTS tasks (id INT, project_id INT, owner_id INT, title VARCHAR, status VARCHAR)",
      "CREATE TABLE IF NOT EXISTS milestones (id INT, project_id INT, name VARCHAR, due_day VARCHAR)",
      "INSERT INTO projects VALUES (1, 'StoreMy UI', 'active'), (2, 'Query Engine', 'planning')",
      "INSERT INTO members VALUES (1, 'Utkarsh', 'Engineer'), (2, 'Maya', 'Designer'), (3, 'Ravi', 'Reviewer')",
      "INSERT INTO tasks VALUES (1, 1, 1, 'Build template picker', 'doing'), (2, 1, 2, 'Polish empty state', 'todo'), (3, 2, 3, 'Review join plan', 'todo')",
      "INSERT INTO milestones VALUES (1, 1, 'Onboarding demo', '2026-05-20'), (2, 2, 'Planner prototype', '2026-06-01')",
    ],
  },
];
