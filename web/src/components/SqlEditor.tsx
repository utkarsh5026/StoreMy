import Editor, { type OnMount } from "@monaco-editor/react";
import { useRef } from "react";

interface Props {
  value: string;
  onChange: (next: string) => void;
  onRun: () => void;
}

// Monaco-backed SQL editor. Cmd/Ctrl+Enter triggers Run via the parent's
// onRun callback so the same shortcut works whether or not the textarea
// has focus.
export function SqlEditor({ value, onChange, onRun }: Props) {
  const onRunRef = useRef(onRun);
  onRunRef.current = onRun;

  const handleMount: OnMount = (editor, monaco) => {
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      onRunRef.current();
    });
  };

  return (
    <div className="editor-host">
      <Editor
        height="100%"
        language="sql"
        theme="vs-dark"
        value={value}
        onChange={(v) => onChange(v ?? "")}
        onMount={handleMount}
        options={{
          minimap: { enabled: false },
          fontSize: 13,
          fontFamily:
            'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace',
          scrollBeyondLastLine: false,
          automaticLayout: true,
          tabSize: 2,
          wordWrap: "on",
          renderLineHighlight: "line",
        }}
      />
    </div>
  );
}
