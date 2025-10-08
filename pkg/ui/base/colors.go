package base

import "github.com/charmbracelet/lipgloss"

// ColorPalette defines a consistent color scheme
type ColorPalette struct {
	Primary   lipgloss.Color
	Secondary lipgloss.Color
	Accent    lipgloss.Color
	Success   lipgloss.Color
	Warning   lipgloss.Color
	Error     lipgloss.Color
	Muted     lipgloss.Color
}

// DarkPalette is the default dark theme palette
var DarkPalette = ColorPalette{
	Primary:   lipgloss.Color("#7C3AED"), // Purple
	Secondary: lipgloss.Color("#06B6D4"), // Cyan
	Accent:    lipgloss.Color("#10B981"), // Emerald
	Success:   lipgloss.Color("#10B981"), // Emerald
	Warning:   lipgloss.Color("#F59E0B"), // Amber
	Error:     lipgloss.Color("#EF4444"), // Red
	Muted:     lipgloss.Color("#94A3B8"), // Slate
}

// LightPalette is an optional light theme palette
var LightPalette = ColorPalette{
	Primary:   lipgloss.Color("#5A56E0"), // Lighter Purple
	Secondary: lipgloss.Color("#EE6FF8"), // Pink
	Accent:    lipgloss.Color("#02BA84"), // Green
	Success:   lipgloss.Color("#02BA84"), // Green
	Warning:   lipgloss.Color("#FF8C00"), // Orange
	Error:     lipgloss.Color("#FF5F56"), // Red
	Muted:     lipgloss.Color("#9B9B9B"), // Gray
}

// AdaptiveColor provides light/dark variants
type AdaptiveColor = lipgloss.AdaptiveColor

// Common adaptive colors used across the application
var (
	AdaptivePrimary = lipgloss.AdaptiveColor{
		Light: string(LightPalette.Primary),
		Dark:  string(DarkPalette.Primary),
	}
	AdaptiveSecondary = lipgloss.AdaptiveColor{
		Light: string(LightPalette.Secondary),
		Dark:  string(DarkPalette.Secondary),
	}
	AdaptiveSuccess = lipgloss.AdaptiveColor{
		Light: string(LightPalette.Success),
		Dark:  string(DarkPalette.Success),
	}
	AdaptiveWarning = lipgloss.AdaptiveColor{
		Light: string(LightPalette.Warning),
		Dark:  string(DarkPalette.Warning),
	}
	AdaptiveError = lipgloss.AdaptiveColor{
		Light: string(LightPalette.Error),
		Dark:  string(DarkPalette.Error),
	}
	AdaptiveMuted = lipgloss.AdaptiveColor{
		Light: string(LightPalette.Muted),
		Dark:  string(DarkPalette.Muted),
	}
)
