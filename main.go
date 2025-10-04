package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"storemy/pkg/database"
	"storemy/pkg/ui"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Configuration struct {
	DatabaseName string
	LogPath      string
	DataDir      string
	DemoMode     bool
	ImportFile   string
}

func main() {
	config := parseArguments()
	showSplashScreen()

	db, err := initializeDatabase(config)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	if config.DemoMode {
		if err := runDemoMode(db); err != nil {
			log.Fatalf("Demo mode failed: %v", err)
		}
	}

	if config.ImportFile != "" {
		if err := importData(db, config.ImportFile); err != nil {
			log.Fatalf("Failed to import data: %v", err)
		}
	}

	if err := startInteractiveMode(db); err != nil {
		log.Fatalf("Failed to start UI: %v", err)
	}
}

// parseArguments processes command-line flags
func parseArguments() Configuration {
	var config Configuration

	flag.StringVar(&config.DatabaseName, "db", "mydb", "Database name")
	flag.StringVar(&config.DataDir, "data", "./data", "Data directory path")
	flag.BoolVar(&config.DemoMode, "demo", false, "Run in demo mode with sample data")
	flag.StringVar(&config.ImportFile, "import", "", "SQL file to import on startup")

	flag.Parse()

	return config
}

// showSplashScreen displays an attractive welcome screen
func showSplashScreen() {
	splash := `
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║        ███████╗████████╗ ██████╗ ██████╗ ███████╗            ║
║        ██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗██╔════╝            ║
║        ███████╗   ██║   ██║   ██║██████╔╝█████╗              ║
║        ╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝              ║
║        ███████║   ██║   ╚██████╔╝██║  ██║███████╗            ║
║        ╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝            ║
║                                                              ║
║                   ███╗   ███╗██╗   ██╗                       ║
║                   ████╗ ████║╚██╗ ██╔╝                       ║
║                   ██╔████╔██║ ╚████╔╝                        ║
║                   ██║╚██╔╝██║  ╚██╔╝                         ║
║                   ██║ ╚═╝ ██║   ██║                          ║
║                   ╚═╝     ╚═╝   ╚═╝                          ║
║                                                              ║
║             A Beautiful Database Built From Scratch          ║
║                       With Love in Go 🚀                     ║
╚══════════════════════════════════════════════════════════════╝
`

	style := lipgloss.NewStyle().
		Foreground(lipgloss.Color("#7C3AED")).
		Bold(true)

	fmt.Println(style.Render(splash))
	time.Sleep(2 * time.Second)
}

// initializeDatabase creates and sets up the database database
func initializeDatabase(config Configuration) (*database.Database, error) {
	fmt.Printf("🔧 Initializing database '%s'...\n", config.DatabaseName)

	fullPath := filepath.Join(config.DataDir, config.DatabaseName)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %v", err)
	}

	db, err := database.NewDatabase(config.DatabaseName, config.DataDir, config.LogPath)
	if err != nil {
		return nil, err
	}

	fmt.Println("✅ Database initialized successfully!")
	return db, nil
}

// startInteractiveMode launches the Bubble Tea UI
func startInteractiveMode(db *database.Database) error {
	model := ui.NewModel(db)

	p := tea.NewProgram(
		model,
		tea.WithAltScreen(),
		tea.WithMouseCellMotion(),
	)

	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error running program: %v", err)
	}

	return nil
}

// runDemoMode sets up sample tables and data
func runDemoMode(db *database.Database) error {
	fmt.Println("\n🎮 Running demo mode - Creating sample database...")

	demoQueries := []string{
		`CREATE TABLE users (
			id INT,
			name VARCHAR,
			email VARCHAR,
			age INT,
			created_at VARCHAR
		)`,
		`CREATE TABLE products (
			id INT,
			name VARCHAR,
			category VARCHAR,
			price FLOAT,
			stock INT
		)`,
		`CREATE TABLE orders (
			id INT,
			user_id INT,
			product_id INT,
			quantity INT,
			total FLOAT,
			status VARCHAR
		)`,

		`INSERT INTO users (id, name, email, age, created_at) VALUES 
			(1, 'Alice Johnson', 'alice@example.com', 28, '2024-01-15')`,
		`INSERT INTO users (id, name, email, age, created_at) VALUES 
			(2, 'Bob Smith', 'bob@example.com', 35, '2024-01-20')`,
		`INSERT INTO users (id, name, email, age, created_at) VALUES 
			(3, 'Charlie Brown', 'charlie@example.com', 42, '2024-02-01')`,
		`INSERT INTO users (id, name, email, age, created_at) VALUES 
			(4, 'Diana Prince', 'diana@example.com', 31, '2024-02-10')`,
		`INSERT INTO users (id, name, email, age, created_at) VALUES 
			(5, 'Eve Wilson', 'eve@example.com', 26, '2024-02-15')`,

		// Insert sample products
		`INSERT INTO products (id, name, category, price, stock) VALUES 
			(1, 'Laptop Pro', 'Electronics', 1299.99, 50)`,
		`INSERT INTO products (id, name, category, price, stock) VALUES 
			(2, 'Wireless Mouse', 'Electronics', 29.99, 200)`,
		`INSERT INTO products (id, name, category, price, stock) VALUES 
			(3, 'Office Chair', 'Furniture', 399.99, 75)`,
		`INSERT INTO products (id, name, category, price, stock) VALUES 
			(4, 'Standing Desk', 'Furniture', 599.99, 30)`,
		`INSERT INTO products (id, name, category, price, stock) VALUES 
			(5, 'Coffee Maker', 'Appliances', 79.99, 100)`,

		// Insert sample orders
		`INSERT INTO orders (id, user_id, product_id, quantity, total, status) VALUES 
			(1, 1, 1, 1, 1299.99, 'completed')`,
		`INSERT INTO orders (id, user_id, product_id, quantity, total, status) VALUES 
			(2, 2, 2, 2, 59.98, 'completed')`,
		`INSERT INTO orders (id, user_id, product_id, quantity, total, status) VALUES 
			(3, 3, 3, 1, 399.99, 'processing')`,
		`INSERT INTO orders (id, user_id, product_id, quantity, total, status) VALUES 
			(4, 1, 5, 1, 79.99, 'completed')`,
		`INSERT INTO orders (id, user_id, product_id, quantity, total, status) VALUES 
			(5, 4, 4, 1, 599.99, 'shipped')`,
	}

	for i, query := range demoQueries {
		progress := float64(i+1) / float64(len(demoQueries)) * 100
		fmt.Printf("\r📊 Progress: %.0f%% ", progress)

		_, err := db.ExecuteQuery(query)
		if err != nil {
			return fmt.Errorf("failed to execute demo query: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\n✅ Demo database created successfully!")
	fmt.Println("\n📝 Sample queries you can try:")
	fmt.Println("  • SELECT * FROM users")
	fmt.Println("  • SELECT name, age FROM users WHERE age > 30")
	fmt.Println("  • SELECT * FROM products WHERE price < 100")
	fmt.Println("  • SELECT u.name, p.name, o.total FROM orders o")
	fmt.Println("    JOIN users u ON u.id = o.user_id")
	fmt.Println("    JOIN products p ON p.id = o.product_id")
	fmt.Println()

	return nil
}

// importData loads SQL statements from a file
func importData(db *database.Database, filename string) error {
	fmt.Printf("📥 Importing data from %s...\n", filename)
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read import file: %v", err)
	}
	statements := strings.Split(string(content), ";")
	successCount := 0
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		_, err := db.ExecuteQuery(stmt)
		if err != nil {
			fmt.Printf("⚠️  Failed to execute: %s\n   Error: %v\n",
				truncateString(stmt, 50), err)
		} else {
			successCount++
		}
	}

	fmt.Printf("✅ Import completed: %d/%d statements successful\n",
		successCount, len(statements))

	return nil
}

// truncateString limits string length for display
func truncateString(s string, maxLen int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
