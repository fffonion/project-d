import { useEffect, useMemo, useState } from "react";
import "@xyflow/react/dist/style.css";

import { DebugSessionsView } from "@/app/components/DebugSessionsView";
import { EdgeDetailView } from "@/app/components/EdgeDetailView";
import { EdgeListView } from "@/app/components/EdgeListView";
import { NavBar } from "@/app/components/NavBar";
import { ProgramDetailView } from "@/app/components/ProgramDetailView";
import { ProgramListView } from "@/app/components/ProgramListView";
import { useComposer } from "@/app/hooks/useComposer";
import { useDebugSessions } from "@/app/hooks/useDebugSessions";
import { useEdges } from "@/app/hooks/useEdges";
import { usePrograms } from "@/app/hooks/usePrograms";
import type { Section } from "@/app/types";
import { Card, CardContent } from "@/components/ui/card";

export default function App() {
  const [section, setSection] = useState<Section>("edges");
  const [error, setError] = useState("");

  const composer = useComposer({ onError: setError });
  const edges = useEdges({ onError: setError });
  const programs = usePrograms({
    onError: setError,
    showProgramsSection: () => setSection("programs"),
    onProgramDeleted: edges.clearApplyProgramForDeletedProgram,
    composer
  });
  const debugSessions = useDebugSessions({
    onError: setError,
    edgeSummaries: edges.edgeSummaries,
    showDebugSessionsSection: () => setSection("debug_sessions")
  });

  useEffect(() => {
    Promise.all([
      composer.loadBlocks(),
      programs.loadPrograms(),
      edges.loadEdges(),
      debugSessions.loadDebugSessions()
    ]).catch((err) => {
      setError(err instanceof Error ? err.message : "failed to initialize ui");
    });
  }, [composer.loadBlocks, debugSessions.loadDebugSessions, edges.loadEdges, programs.loadPrograms]);

  const selectedApplyProgram = useMemo(
    () => programs.programs.find((program) => program.program_id === edges.applyProgramId) ?? null,
    [edges.applyProgramId, programs.programs]
  );

  return (
    <div className="flex min-h-screen bg-background text-foreground">
      <NavBar
        section={section}
        onSelectEdges={() => {
          setSection("edges");
          edges.setEdgeView("list");
        }}
        onSelectPrograms={() => {
          setSection("programs");
          programs.setProgramView("list");
        }}
        onSelectDebugSessions={() => {
          setSection("debug_sessions");
          debugSessions.setDebugView("list");
        }}
      />

      <main className="min-w-0 flex-1 bg-gradient-to-br from-slate-50 via-white to-sky-50 p-4 lg:p-6">
        {error ? (
          <Card className="mb-4 border-red-300 bg-red-50">
            <CardContent className="p-3 text-sm text-red-700">{error}</CardContent>
          </Card>
        ) : null}

        {section === "edges" ? (
          edges.edgeView === "list" ? (
            <EdgeListView
              edgeStats={edges.edgeStats}
              edgeSearch={edges.edgeSearch}
              onEdgeSearchChange={edges.setEdgeSearch}
              filteredEdges={edges.filteredEdges}
              onSelectEdge={edges.selectEdge}
            />
          ) : (
            <EdgeDetailView
              selectedEdge={edges.selectedEdge}
              onBack={() => edges.setEdgeView("list")}
              programs={programs.programs}
              applyProgramId={edges.applyProgramId}
              onApplyProgramChange={edges.onApplyProgramChange}
              applyVersion={edges.applyVersion}
              onApplyVersionChange={edges.setApplyVersion}
              selectedApplyProgram={selectedApplyProgram}
              applyLoading={edges.applyLoading}
              applyStatus={edges.applyStatus}
              onApplyProgram={() => {
                edges.applyProgramToEdge(programs.programs).catch(() => {
                  // handled by callback
                });
              }}
            />
          )
        ) : section === "programs" ? (
          programs.programView === "list" ? (
            <ProgramListView
              creatingProgram={programs.creatingProgram}
              onCreateProgram={programs.createProgram}
              programSearch={programs.programSearch}
              onProgramSearchChange={programs.setProgramSearch}
              filteredPrograms={programs.filteredPrograms}
              onSelectProgram={programs.selectProgram}
            />
          ) : (
            <ProgramDetailView
              selectedProgram={programs.selectedProgram}
              programNameDraft={programs.programNameDraft}
              onProgramNameDraftChange={programs.setProgramNameDraft}
              selectedVersion={programs.selectedVersion}
              onSelectVersion={programs.selectProgramVersion}
              renamingProgram={programs.renamingProgram}
              onRenameProgram={programs.renameProgram}
              deletingProgram={programs.deletingProgram}
              onDeleteProgram={() => {
                if (!programs.selectedProgram) {
                  return;
                }
                programs.deleteProgram(programs.selectedProgram.program_id, programs.selectedProgram.name).catch(() => {
                  // handled by callback
                });
              }}
              savingVersion={programs.savingVersion}
              canSaveVersion={composer.isCodeEditMode || composer.nodes.length > 0}
              onSaveVersion={programs.saveProgramVersion}
              graphStatus={composer.graphStatus}
              onBackToPrograms={() => programs.setProgramView("list")}
              isCodeEditMode={composer.isCodeEditMode}
              onExitCodeEditMode={() => composer.setIsCodeEditMode(false)}
              onEnterCodeEditMode={() => composer.setIsCodeEditMode(true)}
              source={composer.source}
              activeFlavor={composer.activeFlavor}
              rendering={composer.rendering}
              onFlavorChange={composer.setActiveFlavor}
              onSourceChange={composer.updateSourceText}
              selectedProgramId={programs.selectedProgramId}
              graphCanvasRevision={composer.graphCanvasRevision}
              nodes={composer.nodes}
              edges={composer.edges}
              onNodesChange={composer.onNodesChange}
              onEdgesChange={composer.onEdgesChange}
              onConnect={composer.onConnect}
              onInit={composer.onFlowInit}
              onMoveEnd={composer.onFlowMoveEnd}
              onCanvasDrop={composer.onCanvasDrop}
              selectedNodeCount={composer.selectedNodeCount}
              selectedEdgeCount={composer.selectedEdgeCount}
              paletteMinimized={composer.paletteMinimized}
              onTogglePaletteMinimized={() => composer.setPaletteMinimized((value) => !value)}
              codePanelMinimized={composer.codePanelMinimized}
              onToggleCodePanelMinimized={() => composer.setCodePanelMinimized((value) => !value)}
              definitions={composer.filteredDefinitions}
              search={composer.search}
              onSearchChange={composer.setSearch}
              onPaletteDragStart={composer.onPaletteDragStart}
              onAddNode={composer.addNode}
            />
          )
        ) : (
          <DebugSessionsView
            debugView={debugSessions.debugView}
            onBackToList={() => debugSessions.setDebugView("list")}
            debugEdgeId={debugSessions.debugEdgeId}
            onDebugEdgeIdChange={debugSessions.setDebugEdgeId}
            edgeSummaries={edges.edgeSummaries}
            debugHeaderName={debugSessions.debugHeaderName}
            onDebugHeaderNameChange={debugSessions.setDebugHeaderName}
            onCreateDebugSession={debugSessions.createDebugSession}
            debugCreating={debugSessions.debugCreating}
            startDisabledReason={debugSessions.debugStartDisabledReason}
            debugSessionsSorted={debugSessions.debugSessionsSorted}
            selectedDebugSessionId={debugSessions.selectedDebugSessionId}
            onSelectDebugSession={debugSessions.selectDebugSession}
            selectedDebugSession={debugSessions.selectedDebugSession}
            runDebugCommand={debugSessions.runDebugCommand}
            onStopDebugSession={debugSessions.stopDebugSession}
            debugCommandLoading={debugSessions.debugCommandLoading}
            onDebugEditorMount={debugSessions.onDebugEditorMount}
            debugHoveredVar={debugSessions.debugHoveredVar}
            debugHoverValue={debugSessions.debugHoverValue}
          />
        )}
      </main>
    </div>
  );
}
