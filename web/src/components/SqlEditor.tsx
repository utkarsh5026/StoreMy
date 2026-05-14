import Editor, { type OnMount } from "@monaco-editor/react";
import { useRef } from "react";

interface Props {
  value: string;
  onChange: (next: string) => void;
  onRun: () => void;
}

export function SqlEditor({ value, onChange, onRun }: Props) {
  const onRunRef = useRef(onRun);
  onRunRef.current = onRun;

  const handleMount: OnMount = (editor, monaco) => {
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      onRunRef.current();
    });
  };

  return (
    <div className="flex-1 min-h-0">
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
