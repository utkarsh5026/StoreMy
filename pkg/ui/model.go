package ui

import (
	"fmt"
	"storemy/pkg/database"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/help"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Model represents the application state
type Model struct {
	database    *database.Database
	queryEditor textarea.Model
	resultView  viewport.Model
	resultTable table.Model
	spinner     spinner.Model
	help        help.Model

	width        int
	height       int
	executing    bool
	showHelp     bool
	lastResult   database.QueryResult
	lastError    error
	queryHistory []string

	lastQueryTime time.Duration
	keys          keyMap
}

func NewModel(db *database.Database) Model {
	ta := textarea.New()
	ta.Placeholder = "Enter your SQL query here..."
	ta.CharLimit = 5000
	ta.ShowLineNumbers = true
	ta.SetHeight(6)
	ta.Focus()

	ta.FocusedStyle.CursorLine = lipgloss.NewStyle().Background(bgLight)
	ta.FocusedStyle.Placeholder = lipgloss.NewStyle().Foreground(textMuted)
	ta.FocusedStyle.Text = lipgloss.NewStyle().Foreground(textPrimary)
	ta.FocusedStyle.LineNumber = lipgloss.NewStyle().Foreground(textMuted)

	vp := viewport.New(80, 10)
	vp.Style = resultStyle

	t := table.New(
		table.WithColumns([]table.Column{{Title: "Results", Width: 80}}),
		table.WithRows([]table.Row{}),
		table.WithFocused(false),
		table.WithHeight(10),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(primaryColor).
		BorderBottom(true).
		Bold(true).
		Foreground(primaryColor)
	s.Selected = s.Selected.
		Foreground(bgDark).
		Background(secondaryColor).
		Bold(false)
	t.SetStyles(s)

	// Initialize spinner
	sp := spinner.New()
	sp.Spinner = spinner.Points
	sp.Style = lipgloss.NewStyle().Foreground(primaryColor)

	return Model{
		database:     db,
		queryEditor:  ta,
		resultView:   vp,
		resultTable:  t,
		spinner:      sp,
		help:         help.New(),
		keys:         keys,
		queryHistory: make([]string, 0),
		showHelp:     false,
	}
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		textarea.Blink,
	)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.updateLayout()

	case tea.KeyMsg:
		if m.executing {
			return m, nil // Ignore input while executing
		}

		switch {
		case key.Matches(msg, m.keys.Quit):
			return m, tea.Quit

		case key.Matches(msg, m.keys.Execute):
			query := m.queryEditor.Value()
			if strings.TrimSpace(query) != "" {
				m.executing = true
				return m, m.executeQuery(query)
			}

		case key.Matches(msg, m.keys.Clear):
			m.queryEditor.SetValue("")
			m.lastResult = database.QueryResult{}
			m.lastError = nil

		case key.Matches(msg, m.keys.ShowTables):
			m.executing = true
			return m, m.executeQuery("SHOW TABLES")

		case key.Matches(msg, m.keys.ShowStats):
			return m, m.showStatistics()

		case key.Matches(msg, m.keys.Help):
			m.showHelp = !m.showHelp
		}

	case queryResultMsg:
		m.executing = false
		m.lastResult = msg.result
		m.lastError = msg.err
		m.lastQueryTime = msg.duration

		if msg.err == nil {
			m.queryHistory = append(m.queryHistory, msg.query)
			m.updateResultDisplay()
		}

	case spinner.TickMsg:
		if m.executing {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
	}

	// Update sub-components
	if !m.executing {
		var cmd tea.Cmd
		m.queryEditor, cmd = m.queryEditor.Update(msg)
		cmds = append(cmds, cmd)

		m.resultView, cmd = m.resultView.Update(msg)
		cmds = append(cmds, cmd)

		m.resultTable, cmd = m.resultTable.Update(msg)
		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func (m Model) View() string {
	var sections []string

	// Header with gradient effect
	header := m.renderHeader()
	sections = append(sections, header)

	// Query editor section
	editorSection := m.renderQueryEditor()
	sections = append(sections, editorSection)

	// Results or executing spinner
	switch {
	case m.executing:
		sections = append(sections, m.renderExecuting())
	case m.lastError != nil:
		sections = append(sections, m.renderError())
	case len(m.lastResult.Rows) > 0:
		sections = append(sections, m.renderResultTable())
	case m.lastResult.Message != "":
		sections = append(sections, m.renderMessage())
	}

	// Status bar
	sections = append(sections, m.renderStatusBar())

	// Help overlay
	if m.showHelp {
		sections = append(sections, m.renderHelp())
	}

	return appStyle.Render(strings.Join(sections, "\n"))
}

func (m Model) renderHelp() string {
	helpText := m.help.FullHelpView([][]key.Binding{
		{
			m.keys.Execute,
			m.keys.Clear,
			m.keys.ShowTables,
			m.keys.ShowStats,
			m.keys.Help,
			m.keys.Quit,
		},
	})

	return lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		BorderForeground(primaryColor).
		Padding(1, 2).
		Background(bgMedium).
		Render(helpText)
}

func (m Model) renderHeader() string {
	dbInfo := m.database.GetStatistics()

	title := titleStyle.Render("🚀 StoreMyDB Terminal")
	badge := dbBadgeStyle.Render(fmt.Sprintf("📊 %s", dbInfo.Name))
	tables := lipgloss.NewStyle().
		Foreground(textSecondary).
		Render(fmt.Sprintf("Tables: %d | Queries: %d",
			dbInfo.TableCount, dbInfo.QueriesExecuted))

	header := lipgloss.JoinHorizontal(
		lipgloss.Left,
		title,
		"  ",
		badge,
		"  ",
		tables,
	)

	// Add a gradient separator
	separatorWidth := m.width - 4
	if separatorWidth < 0 {
		separatorWidth = 0
	}
	separator := strings.Repeat("─", separatorWidth)
	sepStyle := lipgloss.NewStyle().
		Foreground(bgLight).
		Render(separator)

	return header + "\n" + sepStyle
}

func (m Model) renderQueryEditor() string {
	label := lipgloss.NewStyle().
		Foreground(primaryColor).
		Bold(true).
		Render("SQL Query Editor")

	editor := editorStyle.Render(m.queryEditor.View())

	return fmt.Sprintf("%s\n%s", label, editor)
}

func (m Model) renderExecuting() string {
	content := lipgloss.JoinHorizontal(
		lipgloss.Left,
		m.spinner.View(),
		" Executing query...",
	)

	return lipgloss.NewStyle().
		Foreground(primaryColor).
		Padding(1, 0).
		Render(content)
}

func (m Model) renderError() string {
	icon := errorStyle.Render(" ⚠ ERROR ")
	message := lipgloss.NewStyle().
		Foreground(errorColor).
		Render(m.lastError.Error())

	content := fmt.Sprintf("%s %s", icon, message)

	return lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(errorColor).
		Padding(0, 1).
		Render(content)
}

func (m Model) renderResultTable() string {
	// Create table columns
	columns := make([]table.Column, len(m.lastResult.Columns))
	for i, col := range m.lastResult.Columns {
		width := m.calculateColumnWidth(col, i)
		columns[i] = table.Column{
			Title: col,
			Width: width,
		}
	}

	// Convert result rows to table rows
	rows := make([]table.Row, len(m.lastResult.Rows))
	for i, row := range m.lastResult.Rows {
		rows[i] = table.Row(row)
	}

	// Update table
	m.resultTable.SetColumns(columns)
	m.resultTable.SetRows(rows)

	// Result header
	header := lipgloss.NewStyle().
		Foreground(accentColor).
		Bold(true).
		Render(fmt.Sprintf("✓ Results (%d rows in %v)",
			len(rows), m.lastQueryTime))

	return fmt.Sprintf("%s\n%s", header, m.resultTable.View())
}

func (m Model) renderMessage() string {
	icon := successStyle.Render(" ✓ ")
	message := m.lastResult.Message

	if m.lastResult.RowsAffected > 0 {
		message = fmt.Sprintf("%s (Rows affected: %d)",
			message, m.lastResult.RowsAffected)
	}

	return lipgloss.NewStyle().
		Foreground(accentColor).
		Padding(1, 0).
		Render(fmt.Sprintf("%s %s", icon, message))
}

func (m Model) renderStatusBar() string {
	status := "● Connected"
	statusColor := accentColor

	timer := ""
	if m.lastQueryTime > 0 {
		timer = fmt.Sprintf(" | Last query: %v", m.lastQueryTime)
	}

	helpHint := " | Press Ctrl+H for help"
	content := lipgloss.NewStyle().
		Foreground(statusColor).
		Render(status) +
		lipgloss.NewStyle().
			Foreground(textMuted).
			Render(timer+helpHint)

	return statusBarStyle.
		Width(m.width - 4).
		Render(content)
}

func (m Model) calculateColumnWidth(columnName string, index int) int {
	maxWidth := 30
	minWidth := 10

	// Start with column name length
	width := len(columnName) + 2

	// Check data width
	for _, row := range m.lastResult.Rows {
		if index < len(row) {
			dataWidth := len(row[index]) + 2
			if dataWidth > width {
				width = dataWidth
			}
		}
	}

	// Apply bounds
	if width < minWidth {
		width = minWidth
	} else if width > maxWidth {
		width = maxWidth
	}

	return width
}

// updateLayout adjusts component sizes based on window size
func (m *Model) updateLayout() {
	editorHeight := 6
	resultHeight := m.height - editorHeight - 10 // Leave room for header/status

	m.queryEditor.SetWidth(m.width - 6)
	m.resultView.Width = m.width - 6
	m.resultView.Height = resultHeight
	m.resultTable.SetHeight(resultHeight)
}

func (m *Model) updateResultDisplay() {
	if len(m.lastResult.Rows) > 0 {
		m.resultTable.Focus()
	}
}

type queryResultMsg struct {
	query    string
	result   database.QueryResult
	err      error
	duration time.Duration
}

func (m Model) executeQuery(query string) tea.Cmd {
	return func() tea.Msg {
		start := time.Now()
		result, err := m.database.ExecuteQuery(query)
		duration := time.Since(start)

		return queryResultMsg{
			query:    query,
			result:   result,
			err:      err,
			duration: duration,
		}
	}
}

// showStatistics displays database statistics
func (m Model) showStatistics() tea.Cmd {
	return func() tea.Msg {
		stats := m.database.GetStatistics()

		// Format statistics as a result table
		columns := []string{"Metric", "Value"}
		rows := [][]string{
			{"Database Name", stats.Name},
			{"Total Tables", fmt.Sprintf("%d", stats.TableCount)},
			{"Queries Executed", fmt.Sprintf("%d", stats.QueriesExecuted)},
			{"Transactions", fmt.Sprintf("%d", stats.TransactionsCount)},
			{"Errors", fmt.Sprintf("%d", stats.ErrorCount)},
		}

		// Add table list
		if len(stats.Tables) > 0 {
			rows = append(rows, []string{"Tables", strings.Join(stats.Tables, ", ")})
		}

		return queryResultMsg{
			query: "SHOW STATISTICS",
			result: database.QueryResult{
				Success: true,
				Columns: columns,
				Rows:    rows,
				Message: "Database statistics",
			},
			duration: time.Millisecond * 10,
		}
	}
}
