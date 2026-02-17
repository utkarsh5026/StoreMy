package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"storemy/pkg/database"
	"storemy/pkg/ui"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Configuration struct {
	DatabaseName string
	LogPath      string
	DataDir      string
}

func main() {
	config := parseArguments()
	showSplashScreen()

	db, err := initializeDatabase(config)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	if err := startInteractiveMode(db); err != nil {
		log.Printf("Failed to start UI: %v", err)
	}
}

// parseArguments processes command-line flags
func parseArguments() Configuration {
	var config Configuration

	flag.StringVar(&config.DatabaseName, "db", "mydb", "Database name")
	flag.StringVar(&config.DataDir, "data", "./data", "Data directory path")

	flag.Parse()

	if config.LogPath == "" {
		config.LogPath = filepath.Join(config.DataDir, "logs", "wal.log")
	}

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
	if err := os.MkdirAll(fullPath, 0o755); err != nil {
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
