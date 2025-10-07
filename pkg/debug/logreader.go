package debug

import (
	"fmt"
	"os"
	"storemy/pkg/log"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Styles using Lip Gloss
var (
	// Color palette
	primaryColor   = lipgloss.AdaptiveColor{Light: "#5A56E0", Dark: "#7C79FF"}
	secondaryColor = lipgloss.AdaptiveColor{Light: "#EE6FF8", Dark: "#F780FF"}
	successColor   = lipgloss.AdaptiveColor{Light: "#02BA84", Dark: "#02D98E"}
	warningColor   = lipgloss.AdaptiveColor{Light: "#FF8C00", Dark: "#FFA500"}
	errorColor     = lipgloss.AdaptiveColor{Light: "#FF5F56", Dark: "#FF6B6B"}
	mutedColor     = lipgloss.AdaptiveColor{Light: "#9B9B9B", Dark: "#5C5C5C"}
	bgColor        = lipgloss.AdaptiveColor{Light: "#FFFFFF", Dark: "#1E1E2E"}
	fgColor        = lipgloss.AdaptiveColor{Light: "#1E1E2E", Dark: "#CDD6F4"}

	// Title style
	titleStyle = lipgloss.NewStyle().
			Foreground(primaryColor).
			Bold(true).
			Padding(0, 1).
			MarginBottom(1)

	// Header style
	headerStyle = lipgloss.NewStyle().
			Foreground(secondaryColor).
			Bold(true).
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(primaryColor).
			Padding(0, 1)

	// Selected item style
	selectedItemStyle = lipgloss.NewStyle().
				Foreground(lipgloss.Color("#FFFFFF")).
				Background(primaryColor).
				Bold(true).
				Padding(0, 1)

	// Normal item style
	itemStyle = lipgloss.NewStyle().
			Foreground(fgColor).
			Padding(0, 1)

	// Detail panel style
	detailStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(primaryColor).
			Padding(1, 2).
			MarginTop(1)

	// Key value style
	labelStyle = lipgloss.NewStyle().
			Foreground(secondaryColor).
			Bold(true)

	valueStyle = lipgloss.NewStyle().
			Foreground(fgColor)

	// Help style
	helpStyle = lipgloss.NewStyle().
			Foreground(mutedColor).
			MarginTop(1).
			Padding(0, 1)

	// Status bar style
	statusBarStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(primaryColor).
			Padding(0, 1).
			MarginTop(1)

	// Error style
	errorStyle = lipgloss.NewStyle().
			Foreground(errorColor).
			Bold(true).
			Padding(1)
)

type keyMap struct {
	Up     key.Binding
	Down   key.Binding
	Select key.Binding
	Back   key.Binding
	Quit   key.Binding
}

var keys = keyMap{
	Up: key.NewBinding(
		key.WithKeys("up", "k"),
		key.WithHelp("↑/k", "move up"),
	),
	Down: key.NewBinding(
		key.WithKeys("down", "j"),
		key.WithHelp("↓/j", "move down"),
	),
	Select: key.NewBinding(
		key.WithKeys("enter", " "),
		key.WithHelp("enter/space", "view details"),
	),
	Back: key.NewBinding(
		key.WithKeys("esc", "backspace"),
		key.WithHelp("esc", "back to list"),
	),
	Quit: key.NewBinding(
		key.WithKeys("q", "ctrl+c"),
		key.WithHelp("q", "quit"),
	),
}

type model struct {
	records      []*log.LogRecord
	cursor       int
	selected     *log.LogRecord
	viewport     viewport.Model
	width        int
	height       int
	detailMode   bool
	err          error
	logPath      string
	totalRecords int
}

func initialModel(logPath string) model {
	return model{
		logPath: logPath,
	}
}

func (m model) Init() tea.Cmd {
	return loadRecords(m.logPath)
}

type recordsLoadedMsg struct {
	records []*log.LogRecord
	err     error
}

func loadRecords(logPath string) tea.Cmd {
	return func() tea.Msg {
		reader, err := log.NewLogReader(logPath)
		if err != nil {
			return recordsLoadedMsg{err: err}
		}
		defer reader.Close()

		records, err := reader.ReadAll()
		if err != nil {
			return recordsLoadedMsg{err: err}
		}

		return recordsLoadedMsg{records: records}
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case recordsLoadedMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, tea.Quit
		}
		m.records = msg.records
		m.totalRecords = len(msg.records)
		return m, nil

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.viewport = viewport.New(msg.Width-4, msg.Height-10)
		return m, nil

	case tea.KeyMsg:
		if m.detailMode {
			switch {
			case key.Matches(msg, keys.Back):
				m.detailMode = false
				return m, nil
			case key.Matches(msg, keys.Quit):
				return m, tea.Quit
			}
		} else {
			switch {
			case key.Matches(msg, keys.Quit):
				return m, tea.Quit
			case key.Matches(msg, keys.Up):
				if m.cursor > 0 {
					m.cursor--
				}
			case key.Matches(msg, keys.Down):
				if m.cursor < len(m.records)-1 {
					m.cursor++
				}
			case key.Matches(msg, keys.Select):
				if m.cursor < len(m.records) {
					m.selected = m.records[m.cursor]
					m.detailMode = true
					m.viewport.SetContent(m.renderDetailView())
				}
			}
		}
	}

	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m model) View() string {
	if m.err != nil {
		return errorStyle.Render(fmt.Sprintf("Error: %v\n\nPress q to quit.", m.err))
	}

	if len(m.records) == 0 {
		return "Loading log records...\n"
	}

	var b strings.Builder

	// Title
	title := titleStyle.Render("🗂  Database Write-Ahead Log Viewer")
	b.WriteString(title + "\n\n")

	if m.detailMode {
		// Detail view
		b.WriteString(m.viewport.View())
		b.WriteString("\n\n")
		b.WriteString(helpStyle.Render("Press esc to go back | q to quit"))
	} else {
		// List view
		b.WriteString(m.renderListView())
	}

	// Status bar
	statusBar := m.renderStatusBar()
	b.WriteString("\n" + statusBar)

	return b.String()
}

func (m model) renderListView() string {
	var b strings.Builder

	header := headerStyle.Render(fmt.Sprintf(" Total Records: %d ", m.totalRecords))
	b.WriteString(header + "\n\n")

	// Calculate visible range
	visibleStart := max(0, m.cursor-10)
	visibleEnd := min(len(m.records), visibleStart+20)

	for i := visibleStart; i < visibleEnd; i++ {
		record := m.records[i]

		// Build record line
		recordLine := m.formatRecordLine(record, i)

		// Apply selection style
		if i == m.cursor {
			recordLine = selectedItemStyle.Render("▶ " + recordLine)
		} else {
			recordLine = itemStyle.Render("  " + recordLine)
		}

		b.WriteString(recordLine + "\n")
	}

	b.WriteString("\n")
	b.WriteString(helpStyle.Render("↑/↓: navigate | enter: view details | q: quit"))

	return b.String()
}

func (m model) formatRecordLine(record *log.LogRecord, index int) string {
	// Record type with color
	typeStr := m.colorizeRecordType(record.Type)

	// LSN
	lsnStr := labelStyle.Render("LSN:") + " " + valueStyle.Render(fmt.Sprintf("%d", record.LSN))

	// Transaction ID
	tidStr := ""
	if record.TID != nil {
		tidStr = labelStyle.Render("TID:") + " " + valueStyle.Render(record.TID.String())
	}

	// Timestamp
	timeStr := lipgloss.NewStyle().Foreground(mutedColor).Render(record.Timestamp.Format("15:04:05"))

	return fmt.Sprintf("[%3d] %s │ %s │ %s │ %s", index+1, typeStr, lsnStr, tidStr, timeStr)
}

func (m model) colorizeRecordType(recordType log.LogRecordType) string {
	var color lipgloss.Color
	var icon string
	var name string

	switch recordType {
	case log.BeginRecord:
		color = lipgloss.Color(successColor.Dark)
		icon = "▶"
		name = "BEGIN    "
	case log.CommitRecord:
		color = lipgloss.Color(successColor.Dark)
		icon = "✓"
		name = "COMMIT   "
	case log.AbortRecord:
		color = lipgloss.Color(errorColor.Dark)
		icon = "✗"
		name = "ABORT    "
	case log.UpdateRecord:
		color = lipgloss.Color(warningColor.Dark)
		icon = "⟳"
		name = "UPDATE   "
	case log.InsertRecord:
		color = lipgloss.Color(primaryColor.Dark)
		icon = "+"
		name = "INSERT   "
	case log.DeleteRecord:
		color = lipgloss.Color(errorColor.Dark)
		icon = "−"
		name = "DELETE   "
	case log.CheckpointBegin:
		color = lipgloss.Color(secondaryColor.Dark)
		icon = "◆"
		name = "CKPT BEGIN"
	case log.CheckpointEnd:
		color = lipgloss.Color(secondaryColor.Dark)
		icon = "◇"
		name = "CKPT END  "
	case log.CLRRecord:
		color = lipgloss.Color(mutedColor.Dark)
		icon = "↶"
		name = "CLR      "
	default:
		color = lipgloss.Color(mutedColor.Dark)
		icon = "?"
		name = "UNKNOWN  "
	}

	return lipgloss.NewStyle().Foreground(color).Render(icon + " " + name)
}

func (m model) renderDetailView() string {
	if m.selected == nil {
		return "No record selected"
	}

	var b strings.Builder

	record := m.selected

	// Title
	typeStr := m.colorizeRecordType(record.Type)
	b.WriteString(lipgloss.NewStyle().Bold(true).Foreground(primaryColor).Render("Record Details") + "\n\n")
	b.WriteString(labelStyle.Render("Type: ") + typeStr + "\n\n")

	// LSN and basic info
	b.WriteString(m.renderKeyValue("LSN", fmt.Sprintf("%d", record.LSN)))
	b.WriteString(m.renderKeyValue("Previous LSN", fmt.Sprintf("%d", record.PrevLSN)))
	b.WriteString(m.renderKeyValue("Timestamp", record.Timestamp.Format("2006-01-02 15:04:05")))

	if record.TID != nil {
		b.WriteString(m.renderKeyValue("Transaction ID", record.TID.String()))
	}

	b.WriteString("\n")

	// Type-specific details
	switch record.Type {
	case log.UpdateRecord, log.InsertRecord, log.DeleteRecord:
		if record.PageID != nil {
			b.WriteString(labelStyle.Render("Page Information:") + "\n")
			b.WriteString(m.renderKeyValue("  Table ID", fmt.Sprintf("%d", record.PageID.GetTableID())))
			b.WriteString(m.renderKeyValue("  Page Number", fmt.Sprintf("%d", record.PageID.PageNo())))
			b.WriteString("\n")
		}

		if record.BeforeImage != nil {
			b.WriteString(m.renderKeyValue("Before Image Size", fmt.Sprintf("%d bytes", len(record.BeforeImage))))
		}
		if record.AfterImage != nil {
			b.WriteString(m.renderKeyValue("After Image Size", fmt.Sprintf("%d bytes", len(record.AfterImage))))
		}

	case log.CLRRecord:
		if record.PageID != nil {
			b.WriteString(labelStyle.Render("CLR Information:") + "\n")
			b.WriteString(m.renderKeyValue("  Undo Next LSN", fmt.Sprintf("%d", record.UndoNextLSN)))
			b.WriteString(m.renderKeyValue("  Table ID", fmt.Sprintf("%d", record.PageID.GetTableID())))
			b.WriteString(m.renderKeyValue("  Page Number", fmt.Sprintf("%d", record.PageID.PageNo())))
		}
	}

	return detailStyle.Render(b.String())
}

func (m model) renderKeyValue(key, value string) string {
	return fmt.Sprintf("%s %s\n",
		labelStyle.Render(key+":"),
		valueStyle.Render(value))
}

func (m model) renderStatusBar() string {
	position := fmt.Sprintf("%d/%d", m.cursor+1, m.totalRecords)

	if m.detailMode {
		return statusBarStyle.Render(fmt.Sprintf(" Detail View | Position: %s ", position))
	}

	return statusBarStyle.Render(fmt.Sprintf(" List View | Position: %s | %s ", position, m.logPath))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: logreader <path-to-log-file>")
		os.Exit(1)
	}

	logPath := os.Args[1]

	p := tea.NewProgram(
		initialModel(logPath),
		tea.WithAltScreen(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
