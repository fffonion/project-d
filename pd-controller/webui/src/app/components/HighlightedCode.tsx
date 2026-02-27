import Editor from "@monaco-editor/react";

import { monacoLanguageForFlavor } from "@/app/helpers";
import type { SourceFlavor, UiSourceBundle } from "@/app/types";

export function HighlightedCode({
  flavor,
  source,
  readOnly = true,
  height = "520px",
  onChange
}: {
  flavor: SourceFlavor;
  source: UiSourceBundle;
  readOnly?: boolean;
  height?: string;
  onChange?: (value: string) => void;
}) {
  const language = monacoLanguageForFlavor(flavor);
  const code = source[flavor] ?? "";

  return (
    <div className="h-full overflow-auto rounded-md border border-border">
      <Editor
        height={height}
        language={language}
        value={code}
        theme="vs"
        onChange={(value) => {
          if (onChange) {
            onChange(value ?? "");
          }
        }}
        options={{
          readOnly,
          minimap: { enabled: false },
          scrollBeyondLastLine: false,
          automaticLayout: true,
          wordWrap: "on",
          fontSize: 13,
          lineNumbersMinChars: 3
        }}
      />
    </div>
  );
}
