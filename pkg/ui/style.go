package ui

import (
	"github.com/charmbracelet/lipgloss"
)

var (
	// Primary colors
	primaryColor   = lipgloss.Color("#7C3AED") // Purple
	secondaryColor = lipgloss.Color("#06B6D4") // Cyan
	accentColor    = lipgloss.Color("#10B981") // Emerald
	warningColor   = lipgloss.Color("#F59E0B") // Amber
	errorColor     = lipgloss.Color("#EF4444") // Red

	// Background gradients
	bgDark   = lipgloss.Color("#0F172A")
	bgMedium = lipgloss.Color("#1E293B")
	bgLight  = lipgloss.Color("#334155")

	// Text colors
	textPrimary   = lipgloss.Color("#F8FAFC")
	textSecondary = lipgloss.Color("#CBD5E1")
	textMuted     = lipgloss.Color("#94A3B8")
)

// Styles for different UI components
var (
	appStyle = lipgloss.NewStyle().
			Background(bgDark).
			Foreground(textPrimary).
			Padding(1, 2)

	titleStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("#8B5CF6")).
			Foreground(lipgloss.Color("#FFFFFF")).
			Bold(true).
			Padding(0, 2).
			MarginBottom(1)

	dbBadgeStyle = lipgloss.NewStyle().
			Background(secondaryColor).
			Foreground(bgDark).
			Bold(true).
			Padding(0, 1).
			MarginRight(2)

	statusBarStyle = lipgloss.NewStyle().
			Background(bgMedium).
			Foreground(textSecondary).
			Padding(0, 1)

	successStyle = lipgloss.NewStyle().
			Background(accentColor).
			Foreground(bgDark).
			Bold(true).
			Padding(0, 1)

	errorStyle = lipgloss.NewStyle().
			Background(errorColor).
			Foreground(textPrimary).
			Bold(true).
			Padding(0, 1)

	editorStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(primaryColor).
			Padding(0, 1)

	resultStyle = lipgloss.NewStyle().
			Border(lipgloss.NormalBorder()).
			BorderForeground(bgLight).
			Padding(1)
)
