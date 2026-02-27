import Editor, { type OnMount } from "@monaco-editor/react";

import { debugPhaseClasses, debugPhaseLabel, formatUnixMs, monacoLanguageForFlavor } from "@/app/helpers";
import type {
  DebugSessionDetail,
  DebugSessionSummary,
  EdgeSummary,
  RunDebugCommandFn
} from "@/app/types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

type DebugSessionsViewProps = {
  debugEdgeId: string;
  onDebugEdgeIdChange: (value: string) => void;
  edgeSummaries: EdgeSummary[];
  debugHeaderName: string;
  onDebugHeaderNameChange: (value: string) => void;
  onCreateDebugSession: () => void;
  debugCreating: boolean;
  startDisabledReason: string | null;
  debugSessionsSorted: DebugSessionSummary[];
  selectedDebugSessionId: string | null;
  onSelectDebugSession: (sessionId: string) => Promise<void>;
  selectedDebugSession: DebugSessionDetail | null;
  runDebugCommand: RunDebugCommandFn;
  onStopDebugSession: () => void;
  debugCommandLoading: boolean;
  onDebugEditorMount: OnMount;
  debugHoveredVar: string;
  debugHoverValue: string;
};

export function DebugSessionsView({
  debugEdgeId,
  onDebugEdgeIdChange,
  edgeSummaries,
  debugHeaderName,
  onDebugHeaderNameChange,
  onCreateDebugSession,
  debugCreating,
  startDisabledReason,
  debugSessionsSorted,
  selectedDebugSessionId,
  onSelectDebugSession,
  selectedDebugSession,
  runDebugCommand,
  onStopDebugSession,
  debugCommandLoading,
  onDebugEditorMount,
  debugHoveredVar,
  debugHoverValue
}: DebugSessionsViewProps) {
  return (
    <div className="space-y-4">
      <Card className="border-slate-200/80 bg-white/80 backdrop-blur">
        <CardHeader>
          <CardTitle>Debug Sessions</CardTitle>
          <CardDescription>
            Start a remote debug session, wait for attach, then drive breakpoints and stepping.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid grid-cols-1 gap-3 md:grid-cols-[minmax(180px,1fr)_minmax(220px,1.2fr)_auto] md:items-end">
          <div className="space-y-1">
            <Label htmlFor="debug-edge">Edge</Label>
            <select
              id="debug-edge"
              value={debugEdgeId}
              onChange={(event) => onDebugEdgeIdChange(event.target.value)}
              className="h-9 w-full rounded-md border bg-background px-2 text-sm"
            >
              <option value="">Select edge</option>
              {edgeSummaries.map((edge) => (
                <option key={edge.edge_id} value={edge.edge_id}>
                  {edge.edge_name}
                </option>
              ))}
            </select>
          </div>
          <div className="space-y-1">
            <Label htmlFor="debug-header-name">Header Name</Label>
            <Input
              id="debug-header-name"
              value={debugHeaderName}
              onChange={(event) => onDebugHeaderNameChange(event.target.value)}
              placeholder="x-pd-debug-nonce"
            />
          </div>
          <Button onClick={onCreateDebugSession} disabled={debugCreating || !!startDisabledReason}>
            {debugCreating ? "Creating..." : "Start Session"}
          </Button>
        </CardContent>
        {startDisabledReason ? (
          <CardContent className="pt-0">
            <div className="text-xs text-muted-foreground">{startDisabledReason}</div>
          </CardContent>
        ) : null}
      </Card>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[420px_1fr]">
        <Card className="border-slate-200/80 bg-white/80 backdrop-blur">
          <CardHeader className="pb-3">
            <CardTitle>Session List</CardTitle>
            <CardDescription>Most recent first.</CardDescription>
          </CardHeader>
          <CardContent className="max-h-[72vh] space-y-2 overflow-auto">
            {debugSessionsSorted.map((session) => (
              <button
                key={session.session_id}
                type="button"
                onClick={() => {
                  onSelectDebugSession(session.session_id).catch(() => {
                    // handled by callback
                  });
                }}
                className={`w-full rounded-md border px-3 py-2 text-left transition hover:bg-muted/50 ${
                  selectedDebugSessionId === session.session_id ? "border-primary bg-primary/5" : ""
                }`}
              >
                <div className="flex items-center justify-between gap-2">
                  <div className="truncate text-sm font-semibold">{session.edge_name}</div>
                  <Badge className={`rounded-full px-2 py-0 text-[10px] font-semibold uppercase ${debugPhaseClasses(session.phase)}`}>
                    {debugPhaseLabel(session.phase)}
                  </Badge>
                </div>
                <div className="mt-1 truncate text-xs text-muted-foreground">session={session.session_id}</div>
                <div className="mt-1 text-xs text-muted-foreground">updated={formatUnixMs(session.updated_unix_ms)}</div>
              </button>
            ))}
            {debugSessionsSorted.length === 0 ? (
              <div className="rounded-md border bg-background/70 p-4 text-sm text-muted-foreground">
                No debug sessions yet.
              </div>
            ) : null}
          </CardContent>
        </Card>

        <Card className="border-slate-200/80 bg-white/80 backdrop-blur">
          <CardHeader>
            <CardTitle>Session Detail</CardTitle>
            <CardDescription>
              {selectedDebugSession ? selectedDebugSession.edge_name : "Select a session from the list"}
            </CardDescription>
          </CardHeader>
          <CardContent>
            {selectedDebugSession ? (
              <div className="space-y-4">
                <div className="grid grid-cols-1 gap-2 lg:grid-cols-[1fr_auto] lg:items-start">
                  <div className="space-y-2">
                    <div className="rounded-md border bg-background/70 p-2 text-xs">
                      <div className="flex items-center gap-2">
                        <span className="uppercase tracking-wide text-muted-foreground">Phase</span>
                        <Badge className={`rounded-full px-2 py-0 text-[10px] font-semibold uppercase ${debugPhaseClasses(selectedDebugSession.phase)}`}>
                          {debugPhaseLabel(selectedDebugSession.phase)}
                        </Badge>
                      </div>
                      <div className="mt-1 font-mono">session_id={selectedDebugSession.session_id}</div>
                      <div className="font-mono">edge_id={selectedDebugSession.edge_id}</div>
                      {selectedDebugSession.header_name && selectedDebugSession.nonce_header_value ? (
                        <div className="mt-2 rounded-md border bg-amber-50 p-2 text-[11px] text-amber-800">
                          trigger header:{" "}
                          <span className="font-mono">
                            {selectedDebugSession.header_name}: {selectedDebugSession.nonce_header_value}
                          </span>
                        </div>
                      ) : null}
                      {selectedDebugSession.message ? (
                        <div className="mt-2 text-muted-foreground">{selectedDebugSession.message}</div>
                      ) : null}
                    </div>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <Button variant="outline" onClick={() => runDebugCommand({ kind: "where" })} disabled={debugCommandLoading || selectedDebugSession.phase !== "attached"}>
                      Where
                    </Button>
                    <Button variant="outline" onClick={() => runDebugCommand({ kind: "locals" })} disabled={debugCommandLoading || selectedDebugSession.phase !== "attached"}>
                      Locals
                    </Button>
                    <Button variant="outline" onClick={() => runDebugCommand({ kind: "stack" })} disabled={debugCommandLoading || selectedDebugSession.phase !== "attached"}>
                      Stack
                    </Button>
                    <Button
                      onClick={onStopDebugSession}
                      variant="outline"
                      className="border-rose-300 text-rose-700 hover:bg-rose-50"
                      disabled={debugCommandLoading}
                    >
                      Stop Session
                    </Button>
                  </div>
                </div>

                <div className="flex flex-wrap items-end gap-2">
                  <Button onClick={() => runDebugCommand({ kind: "step" })} disabled={debugCommandLoading || selectedDebugSession.phase !== "attached"}>
                    Step
                  </Button>
                  <Button onClick={() => runDebugCommand({ kind: "next" })} disabled={debugCommandLoading || selectedDebugSession.phase !== "attached"}>
                    Next
                  </Button>
                  <Button onClick={() => runDebugCommand({ kind: "out" })} disabled={debugCommandLoading || selectedDebugSession.phase !== "attached"}>
                    Out
                  </Button>
                  <Button onClick={() => runDebugCommand({ kind: "continue" })} disabled={debugCommandLoading || selectedDebugSession.phase !== "attached"}>
                    Continue
                  </Button>
                </div>

                {selectedDebugSession.phase === "waiting_for_attach" || selectedDebugSession.phase === "waiting_for_start_result" ? (
                  <div className="rounded-md border bg-background/70 p-3 text-sm text-muted-foreground">
                    Waiting for attach. Send a request to the selected edge with the trigger header shown above.
                  </div>
                ) : null}

                {selectedDebugSession.source_code ? (
                  <div className="rounded-md border bg-slate-950 text-slate-100">
                    <div className="border-b border-slate-800 px-3 py-2 text-xs text-slate-300">
                      source={selectedDebugSession.source_flavor ?? "unknown"} current_line={selectedDebugSession.current_line ?? "-"}
                    </div>
                    <div className="h-[68vh]">
                      <Editor
                        onMount={onDebugEditorMount}
                        language={monacoLanguageForFlavor(selectedDebugSession.source_flavor)}
                        value={selectedDebugSession.source_code}
                        theme="vs-dark"
                        options={{
                          readOnly: true,
                          glyphMargin: true,
                          minimap: { enabled: false },
                          scrollBeyondLastLine: false,
                          automaticLayout: true,
                          wordWrap: "on",
                          fontSize: 13,
                          lineDecorationsWidth: 20,
                          lineNumbersMinChars: 3,
                          renderLineHighlight: "none"
                        }}
                      />
                    </div>
                  </div>
                ) : (
                  <div className="rounded-md border bg-background/70 p-3 text-sm text-muted-foreground">
                    No source available for this session. Apply a stored program version first.
                  </div>
                )}

                {debugHoveredVar ? (
                  <div className="rounded-md border bg-background/70 p-2 text-xs">
                    <span className="font-semibold">hover inspect:</span> {debugHoveredVar} ={" "}
                    <span className="font-mono">{debugHoverValue || "(loading)"}</span>
                  </div>
                ) : null}

                {selectedDebugSession.last_output ? (
                  <div className="rounded-md border bg-background/70 p-2">
                    <div className="mb-1 text-[11px] uppercase tracking-wide text-muted-foreground">Last Debugger Output</div>
                    <pre className="max-h-[180px] overflow-auto whitespace-pre-wrap text-xs">{selectedDebugSession.last_output}</pre>
                  </div>
                ) : null}
              </div>
            ) : (
              <div className="rounded-md border bg-background/70 p-4 text-sm text-muted-foreground">
                Select a debug session from the list.
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
