package ui

import (
	"fmt"
	"storemy/pkg/ui/base"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/lipgloss"
)

// Color palette - shared across all readers
var (
	PrimaryColor   = base.AdaptivePrimary
	SecondaryColor = base.AdaptiveSecondary
	SuccessColor   = base.AdaptiveSuccess
	WarningColor   = base.AdaptiveWarning
	ErrorColor     = base.AdaptiveError
	MutedColor     = base.AdaptiveMuted
	BgColor        = lipgloss.AdaptiveColor{Light: "#FFFFFF", Dark: "#1E1E2E"}
	FgColor        = lipgloss.AdaptiveColor{Light: "#1E1E2E", Dark: "#CDD6F4"}
)

// Common styles
var (
	TitleStyle = lipgloss.NewStyle().
			Foreground(PrimaryColor).
			Bold(true).
			Padding(0, 1).
			MarginBottom(1)

	HeaderStyle = lipgloss.NewStyle().
			Foreground(SecondaryColor).
			Bold(true).
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(PrimaryColor).
			Padding(0, 1)

	SelectedItemStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFFFFF")).
				Background(PrimaryColor).
				Bold(true).
				Padding(0, 1)

	ItemStyle = lipgloss.NewStyle().
			Foreground(FgColor).
			Padding(0, 1)

	TableHeaderStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFFFFF")).
				Background(SecondaryColor).
				Bold(true).
				Padding(0, 1)

	CellStyle = lipgloss.NewStyle().
			Foreground(FgColor).
			Padding(0, 1)

	DetailStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(PrimaryColor).
			Padding(1, 2).
			MarginTop(1)

	LabelStyle = lipgloss.NewStyle().
			Foreground(SecondaryColor).
			Bold(true)

	ValueStyle = lipgloss.NewStyle().
			Foreground(FgColor)

	PageInfoStyle = lipgloss.NewStyle().
			Foreground(SuccessColor).
			Bold(true).
			Padding(0, 1)

	HelpStyle = lipgloss.NewStyle().
			Foreground(MutedColor).
			MarginTop(1).
			Padding(0, 1)

	StatusBarStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(PrimaryColor).
			Padding(0, 1).
			MarginTop(1)

	ErrorStyle = lipgloss.NewStyle().
			Foreground(ErrorColor).
			Bold(true).
			Padding(1)

	WarningStyle = lipgloss.NewStyle().
			Foreground(WarningColor).
			Bold(true).
			Padding(0, 1)

	SuccessStyle = lipgloss.NewStyle().
			Foreground(SuccessColor).
			Bold(true).
			Padding(0, 1)
)

// Common key bindings
type CommonKeyMap struct {
	Up     key.Binding
	Down   key.Binding
	Left   key.Binding
	Right  key.Binding
	Select key.Binding
	Back   key.Binding
	Quit   key.Binding
}

var CommonKeys = CommonKeyMap{
	Up: key.NewBinding(
		key.WithKeys("up", "k"),
		key.WithHelp("↑/k", "move up"),
	),
	Down: key.NewBinding(
		key.WithKeys("down", "j"),
		key.WithHelp("↓/j", "move down"),
	),
	Left: key.NewBinding(
		key.WithKeys("left", "h"),
		key.WithHelp("←/h", "move left"),
	),
	Right: key.NewBinding(
		key.WithKeys("right", "l"),
		key.WithHelp("→/l", "move right"),
	),
	Select: key.NewBinding(
		key.WithKeys("enter", " "),
		key.WithHelp("enter/space", "select"),
	),
	Back: key.NewBinding(
		key.WithKeys("esc", "backspace"),
		key.WithHelp("esc", "back"),
	),
	Quit: key.NewBinding(
		key.WithKeys("q", "ctrl+c"),
		key.WithHelp("q", "quit"),
	),
}

// Navigation key bindings
type NavigationKeyMap struct {
	NextPage  key.Binding
	PrevPage  key.Binding
	FirstPage key.Binding
	LastPage  key.Binding
}

var NavigationKeys = NavigationKeyMap{
	NextPage: key.NewBinding(
		key.WithKeys("n", "pagedown"),
		key.WithHelp("n/pgdn", "next page"),
	),
	PrevPage: key.NewBinding(
		key.WithKeys("p", "pageup"),
		key.WithHelp("p/pgup", "prev page"),
	),
	FirstPage: key.NewBinding(
		key.WithKeys("g", "home"),
		key.WithHelp("g/home", "first page"),
	),
	LastPage: key.NewBinding(
		key.WithKeys("G", "end"),
		key.WithHelp("G/end", "last page"),
	),
}

// Helper functions - re-export from base package
var (
	PadString       = base.PadString
	TruncateString  = base.TruncateString
	Max             = base.Max
	Min             = base.Min
	CenterString    = base.CenterString
	RightAlign      = base.RightAlign
)

// RenderError renders an error message with instructions to quit
func RenderError(err error) string {
	return ErrorStyle.Render(lipgloss.JoinVertical(
		lipgloss.Left,
		"Error: "+err.Error(),
		"",
		"Press q to quit.",
	))
}

// RenderStatusBar renders a status bar with the given text
func RenderStatusBar(text string) string {
	return StatusBarStyle.Render(text)
}

// RenderTitle renders a title with an icon
func RenderTitle(icon, title string) string {
	return TitleStyle.Render(icon + "  " + title)
}

// RenderHeaderWithCount renders a header with optional count
func RenderHeaderWithCount(text string, count int) string {
	if count >= 0 {
		return HeaderStyle.Render(fmt.Sprintf(" %s (%d) ", text, count))
	}
	return HeaderStyle.Render(" " + text + " ")
}

// RenderTable renders a simple table with headers and data
func RenderTable(headers []string, data [][]string, colWidths []int, selectedRow int) string {
	var b strings.Builder

	// Render headers
	headerRow := ""
	for i, header := range headers {
		headerRow += TableHeaderStyle.Render(PadString(header, colWidths[i]))
		if i < len(headers)-1 {
			headerRow += " "
		}
	}
	b.WriteString(headerRow + "\n")

	// Render separator
	separator := ""
	for i, width := range colWidths {
		separator += strings.Repeat("─", width+2)
		if i < len(colWidths)-1 {
			separator += "┼"
		}
	}
	b.WriteString(lipgloss.NewStyle().Foreground(MutedColor).Render(separator) + "\n")

	// Render data rows
	for rowIdx, row := range data {
		rowStr := ""
		for colIdx, cell := range row {
			cellContent := PadString(cell, colWidths[colIdx])
			if rowIdx == selectedRow {
				rowStr += SelectedItemStyle.Render(cellContent)
			} else {
				rowStr += CellStyle.Render(cellContent)
			}
			if colIdx < len(row)-1 {
				rowStr += " "
			}
		}
		b.WriteString(rowStr + "\n")
	}

	return b.String()
}
