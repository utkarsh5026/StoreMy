import { create } from "zustand";

type Tab = "results" | "heap";

interface UiState {
  tab: Tab;
  selectedTable: string | null;
  wizardOpen: boolean;
  setTab: (tab: Tab) => void;
  setSelectedTable: (name: string | null) => void;
  openWizard: () => void;
  closeWizard: () => void;
  reset: () => void;
}

export const useUiStore = create<UiState>((set) => ({
  tab: "results",
  selectedTable: null,
  wizardOpen: false,

  setTab: (tab) => set({ tab }),
  setSelectedTable: (selectedTable) => set({ selectedTable }),
  openWizard: () => set({ wizardOpen: true }),
  closeWizard: () => set({ wizardOpen: false }),

  // Call when exiting the workspace so stale UI state doesn't bleed into the next session.
  reset: () => set({ tab: "results", selectedTable: null, wizardOpen: false }),
}));
