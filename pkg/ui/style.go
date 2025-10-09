package ui

import (
	"storemy/pkg/ui/base"

	"github.com/charmbracelet/lipgloss"
)

var (
	// Use base color palette
	palette = base.DarkPalette

	// Primary colors
	primaryColor   = palette.Primary
	secondaryColor = palette.Secondary
	accentColor    = palette.Accent
	errorColor     = palette.Error

	// Background gradients
	bgDark   = lipgloss.Color("#0F172A")
	bgMedium = lipgloss.Color("#1E293B")
	bgLight  = lipgloss.Color("#334155")

	// Text colors
	textPrimary   = lipgloss.Color("#F8FAFC")
	textSecondary = lipgloss.Color("#CBD5E1")
	textMuted     = palette.Muted
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
