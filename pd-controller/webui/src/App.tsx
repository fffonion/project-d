import { useCallback, useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import {
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  type Connection,
  type Edge,
  type EdgeChange,
  type Node,
  type NodeChange,
  type ReactFlowInstance,
  type Viewport
} from "@xyflow/react";
import { type OnMount } from "@monaco-editor/react";
import type * as Monaco from "monaco-editor";
import "@xyflow/react/dist/style.css";

import { DebugSessionsView } from "@/app/components/DebugSessionsView";
import { EdgeDetailView } from "@/app/components/EdgeDetailView";
import { EdgeListView } from "@/app/components/EdgeListView";
import { NavBar } from "@/app/components/NavBar";
import { ProgramDetailView } from "@/app/components/ProgramDetailView";
import { ProgramListView } from "@/app/components/ProgramListView";
import {
  applyConnectedInputs,
  edgeHealth,
  graphPayload,
  looksLikeIdentifier,
  normalizeFlavor,
  normalizeFlowEdges,
  toFlowEdges,
  defaultValues
} from "@/app/helpers";
import {
  type DebugCommandRequest,
  type DebugCommandResponse,
  type DebugSessionDetail,
  type DebugSessionListResponse,
  type DebugSessionSummary,
  type EdgeDetailResponse,
  type EdgeListResponse,
  type EdgeSummary,
  type FlowEdge,
  type FlowNode,
  type ProgramDetailResponse,
  type ProgramListResponse,
  type ProgramSummary,
  type ProgramVersionResponse,
  type QueueResponse,
  type RunDebugCommandFn,
  type RunDebugCommandOptions,
  type Section,
  type SourceFlavor,
  type UiBlockDefinition,
  type UiBlocksResponse,
  type UiGraphNode,
  type UiRenderResponse,
  type UiSourceBundle,
  initialSource
} from "@/app/types";

import { Card, CardContent } from "@/components/ui/card";

export default function App() {
  const makeDefaultProgramName = () => `program-${Date.now()}`;
  const [section, setSection] = useState<Section>("edges");
  const [error, setError] = useState("");

  const [definitions, setDefinitions] = useState<UiBlockDefinition[]>([]);
  const [search, setSearch] = useState("");
  const [nodes, setNodes] = useState<FlowNode[]>([]);
  const [edges, setEdges] = useState<FlowEdge[]>([]);
  const [source, setSource] = useState<UiSourceBundle>(initialSource);
  const [activeFlavor, setActiveFlavor] = useState<SourceFlavor>("rustscript");
  const [rendering, setRendering] = useState(false);
  const [rfInstance, setRfInstance] = useState<ReactFlowInstance<FlowNode, FlowEdge> | null>(null);
  const [idSequence, setIdSequence] = useState(0);
  const [graphStatus, setGraphStatus] = useState("");
  const [graphCanvasRevision, setGraphCanvasRevision] = useState(0);
  const [paletteMinimized, setPaletteMinimized] = useState(false);
  const [codePanelMinimized, setCodePanelMinimized] = useState(false);
  const [isCodeEditMode, setIsCodeEditMode] = useState(false);

  const [programs, setPrograms] = useState<ProgramSummary[]>([]);
  const [selectedProgramId, setSelectedProgramId] = useState<string | null>(null);
  const [selectedProgram, setSelectedProgram] = useState<ProgramDetailResponse | null>(null);
  const [selectedVersion, setSelectedVersion] = useState<number | null>(null);
  const [programView, setProgramView] = useState<"list" | "composer">("list");
  const [programSearch, setProgramSearch] = useState("");
  const [programNameDraft, setProgramNameDraft] = useState("");
  const [newProgramName, setNewProgramName] = useState(makeDefaultProgramName);
  const [creatingProgram, setCreatingProgram] = useState(false);
  const [savingVersion, setSavingVersion] = useState(false);
  const [renamingProgram, setRenamingProgram] = useState(false);
  const [deletingProgram, setDeletingProgram] = useState(false);

  const [edgeSummaries, setEdgeSummaries] = useState<EdgeSummary[]>([]);
  const [edgeView, setEdgeView] = useState<"list" | "detail">("list");
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<EdgeDetailResponse | null>(null);
  const [edgeSearch, setEdgeSearch] = useState("");
  const [applyProgramId, setApplyProgramId] = useState<string>("");
  const [applyVersion, setApplyVersion] = useState<string>("latest");
  const [applyLoading, setApplyLoading] = useState(false);
  const [applyStatus, setApplyStatus] = useState("");

  const [debugSessions, setDebugSessions] = useState<DebugSessionSummary[]>([]);
  const [selectedDebugSessionId, setSelectedDebugSessionId] = useState<string | null>(null);
  const [selectedDebugSession, setSelectedDebugSession] = useState<DebugSessionDetail | null>(null);
  const [debugEdgeId, setDebugEdgeId] = useState<string>("");
  const [debugHeaderName, setDebugHeaderName] = useState<string>("x-pd-debug-nonce");
  const [debugCreating, setDebugCreating] = useState(false);
  const [debugCommandLoading, setDebugCommandLoading] = useState(false);
  const [debugHoveredVar, setDebugHoveredVar] = useState<string>("");
  const [debugHoverValue, setDebugHoverValue] = useState<string>("");
  const hydratingGraphRef = useRef(false);
  const flowZoomRef = useRef(0.5);
  const programLoadSeqRef = useRef(0);
  const programLoadAbortRef = useRef<AbortController | null>(null);
  const hydrationTimerRef = useRef<number | null>(null);
  const graphSnapshotRef = useRef<{
    nodes: FlowNode[];
    edges: FlowEdge[];
    source: UiSourceBundle;
    selectedVersion: number | null;
    selectedProgramId: string | null;
  }>({
    nodes: [],
    edges: [],
    source: initialSource,
    selectedVersion: null,
    selectedProgramId: null
  });
  const debugEditorRef = useRef<Monaco.editor.IStandaloneCodeEditor | null>(null);
  const debugMonacoRef = useRef<typeof import("monaco-editor") | null>(null);
  const debugDecorationIdsRef = useRef<string[]>([]);
  const debugHoverProviderDisposableRef = useRef<Monaco.IDisposable | null>(null);
  const selectedDebugSessionRef = useRef<DebugSessionDetail | null>(null);
  const debugCommandLoadingRef = useRef(false);
  const runDebugCommandRef = useRef<RunDebugCommandFn | null>(null);
  const debugHoverCacheRef = useRef<Map<string, string>>(new Map());

  const definitionMap = useMemo(() => {
    const map = new Map<string, UiBlockDefinition>();
    for (const definition of definitions) {
      map.set(definition.id, definition);
    }
    return map;
  }, [definitions]);

  const filteredDefinitions = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) {
      return definitions;
    }
    return definitions.filter((definition) =>
      `${definition.title} ${definition.category} ${definition.description}`.toLowerCase().includes(term)
    );
  }, [definitions, search]);

  const clearHydrationTimer = useCallback(() => {
    if (hydrationTimerRef.current !== null) {
      window.clearTimeout(hydrationTimerRef.current);
      hydrationTimerRef.current = null;
    }
  }, []);

  const applyPreferredZoom = useCallback((instance: ReactFlowInstance<FlowNode, FlowEdge> | null) => {
    if (!instance) {
      return;
    }
    const zoom = Number.isFinite(flowZoomRef.current) ? flowZoomRef.current : 0.5;
    const clamped = Math.min(2, Math.max(0.2, zoom));
    instance.zoomTo(clamped, { duration: 120 });
  }, []);

  const bumpGraphCanvasRevision = useCallback(() => {
    setGraphCanvasRevision((value) => value + 1);
  }, []);

  const updateSourceText = useCallback((flavor: SourceFlavor, value: string) => {
    setSource((curr) => ({ ...curr, [flavor]: value }));
  }, []);

  useEffect(() => {
    graphSnapshotRef.current = {
      nodes,
      edges,
      source,
      selectedVersion,
      selectedProgramId
    };
  }, [edges, nodes, selectedProgramId, selectedVersion, source]);

  useEffect(() => {
    return () => {
      programLoadAbortRef.current?.abort();
      clearHydrationTimer();
      hydratingGraphRef.current = false;
    };
  }, [clearHydrationTimer]);

  const loadBlocks = useCallback(async () => {
    const response = await fetch("/v1/ui/blocks");
    if (!response.ok) {
      throw new Error(`failed to load blocks (${response.status})`);
    }
    const data = (await response.json()) as UiBlocksResponse;
    setDefinitions(data.blocks);
  }, []);

  const loadPrograms = useCallback(async () => {
    const response = await fetch("/v1/programs");
    if (!response.ok) {
      throw new Error(`failed to load programs (${response.status})`);
    }
    const data = (await response.json()) as ProgramListResponse;
    setPrograms(data.programs);
  }, []);

  const loadEdges = useCallback(async () => {
    const response = await fetch("/v1/edges");
    if (!response.ok) {
      throw new Error(`failed to load edges (${response.status})`);
    }
    const data = (await response.json()) as EdgeListResponse;
    setEdgeSummaries(data.edges);
  }, []);

  const loadDebugSessions = useCallback(async () => {
    const response = await fetch("/v1/debug-sessions");
    if (!response.ok) {
      throw new Error(`failed to load debug sessions (${response.status})`);
    }
    const data = (await response.json()) as DebugSessionListResponse;
    setDebugSessions(data.sessions);
  }, []);

  useEffect(() => {
    Promise.all([loadBlocks(), loadPrograms(), loadEdges(), loadDebugSessions()]).catch((err) => {
      setError(err instanceof Error ? err.message : "failed to initialize ui");
    });
  }, [loadBlocks, loadEdges, loadPrograms, loadDebugSessions]);

  const removeNode = useCallback((nodeId: string) => {
    setNodes((curr) => curr.filter((node) => node.id !== nodeId));
    setEdges((curr) => curr.filter((edge) => edge.source !== nodeId && edge.target !== nodeId));
  }, []);

  const updateNodeValue = useCallback((nodeId: string, key: string, value: string) => {
    setNodes((curr) =>
      curr.map((node) =>
        node.id === nodeId
          ? {
              ...node,
              data: {
                ...node.data,
                values: { ...node.data.values, [key]: value }
              }
            }
          : node
      )
    );
  }, []);

  const toFlowNodes = useCallback(
    (graphNodes: UiGraphNode[]) => {
      const loadedNodes: FlowNode[] = [];
      let maxId = 0;
      let skippedNodes = 0;
      for (let index = 0; index < graphNodes.length; index += 1) {
        const graphNode = graphNodes[index];
        const definition = definitionMap.get(graphNode.block_id);
        if (!definition) {
          skippedNodes += 1;
          continue;
        }
        const mergedValues = { ...defaultValues(definition), ...graphNode.values };
        loadedNodes.push({
          id: graphNode.id,
          type: "blockNode",
          position: graphNode.position ?? { x: 120 + (index % 4) * 72, y: 120 + Math.floor(index / 4) * 140 },
          data: {
            blockId: definition.id,
            definition,
            values: mergedValues,
            connectedInputs: {},
            onValueChange: updateNodeValue,
            onDelete: removeNode
          }
        });
        const numeric = Number.parseInt(graphNode.id.replace("node-", ""), 10);
        if (!Number.isNaN(numeric)) {
          maxId = Math.max(maxId, numeric);
        }
      }
      setIdSequence(maxId);
      return { loadedNodes, skippedNodes };
    },
    [definitionMap, removeNode, updateNodeValue]
  );

  const loadProgramDetail = useCallback(
    async (programId: string, preferredVersion?: number | null) => {
      const requestSeq = programLoadSeqRef.current + 1;
      programLoadSeqRef.current = requestSeq;
      programLoadAbortRef.current?.abort();
      const controller = new AbortController();
      programLoadAbortRef.current = controller;
      const isCurrent = () => programLoadSeqRef.current === requestSeq && !controller.signal.aborted;

      try {
        const detailResp = await fetch(`/v1/programs/${programId}`, { signal: controller.signal });
        if (!isCurrent()) {
          return;
        }
        if (!detailResp.ok) {
          throw new Error(`failed to load program (${detailResp.status})`);
        }
        const detail = (await detailResp.json()) as ProgramDetailResponse;
        if (!isCurrent()) {
          return;
        }
        setSelectedProgram(detail);
        setProgramNameDraft(detail.name);

        if (detail.versions.length === 0) {
          clearHydrationTimer();
          hydratingGraphRef.current = false;
          setSelectedVersion(0);
          setIsCodeEditMode(false);
          setNodes([]);
          setEdges([]);
          setSource(initialSource);
          bumpGraphCanvasRevision();
          setGraphStatus("draft v0");
          return;
        }

        const versionToLoad =
          preferredVersion && detail.versions.some((item) => item.version === preferredVersion)
            ? preferredVersion
            : detail.versions[detail.versions.length - 1].version;

        const versionResp = await fetch(`/v1/programs/${programId}/versions/${versionToLoad}`, {
          signal: controller.signal
        });
        if (!isCurrent()) {
          return;
        }
        if (!versionResp.ok) {
          throw new Error(`failed to load program version (${versionResp.status})`);
        }
        const version = (await versionResp.json()) as ProgramVersionResponse;
        if (!isCurrent()) {
          return;
        }
        setSelectedVersion(version.detail.version);
        setActiveFlavor(normalizeFlavor(version.detail.flavor));
        setSource(version.detail.source);
        setIsCodeEditMode(!version.detail.flow_synced);
        if (!version.detail.flow_synced) {
          hydratingGraphRef.current = false;
          setNodes([]);
          setEdges([]);
          bumpGraphCanvasRevision();
          setGraphStatus(`loaded v${version.detail.version} (code edited)`);
          return;
        }
        hydratingGraphRef.current = true;
        const { loadedNodes, skippedNodes } = toFlowNodes(version.detail.nodes);
        if (!isCurrent()) {
          hydratingGraphRef.current = false;
          return;
        }
        if (version.detail.nodes.length === 0) {
          hydratingGraphRef.current = false;
          throw new Error("loaded program version has no nodes; keeping previous graph");
        }
        if (version.detail.nodes.length > 0 && loadedNodes.length === 0) {
          hydratingGraphRef.current = false;
          throw new Error(
            `failed to load program graph: ${skippedNodes} node(s) use unknown block types`
          );
        }
        const loadedEdges = toFlowEdges(version.detail.edges, loadedNodes);
        if (version.detail.edges.length > 0 && loadedEdges.length === 0) {
          setError("loaded graph has edges that do not match current block handles; showing nodes only");
        }
        setNodes(loadedNodes);
        setEdges(loadedEdges);
        setGraphStatus(`loaded v${version.detail.version}`);
        bumpGraphCanvasRevision();
        clearHydrationTimer();
        hydrationTimerRef.current = window.setTimeout(() => {
          if (!isCurrent()) {
            return;
          }
          hydratingGraphRef.current = false;
          rfInstance?.fitView({ padding: 0.35 });
          applyPreferredZoom(rfInstance);
          hydrationTimerRef.current = null;
        }, 80);
      } catch (err) {
        clearHydrationTimer();
        hydratingGraphRef.current = false;
        if ((err as { name?: string }).name === "AbortError") {
          return;
        }
        throw err;
      } finally {
        if (programLoadAbortRef.current === controller) {
          programLoadAbortRef.current = null;
        }
      }
    },
    [applyPreferredZoom, bumpGraphCanvasRevision, clearHydrationTimer, rfInstance, toFlowNodes]
  );

  const selectProgram = useCallback(
    async (programId: string) => {
      setSelectedProgramId(programId);
      setProgramView("composer");
      setGraphStatus("");
      setError("");
      try {
        await loadProgramDetail(programId);
      } catch (err) {
        setError(err instanceof Error ? err.message : "failed to load program");
      }
    },
    [loadProgramDetail]
  );

  const selectProgramVersion = useCallback(
    async (versionValue: string) => {
      if (!selectedProgramId) {
        return;
      }
      const version = Number.parseInt(versionValue, 10);
      if (Number.isNaN(version)) {
        return;
      }
      if (version === 0) {
        setSelectedVersion(0);
        setIsCodeEditMode(false);
        setGraphStatus("draft v0");
        return;
      }
      setError("");
      try {
        await loadProgramDetail(selectedProgramId, version);
      } catch (err) {
        setError(err instanceof Error ? err.message : "failed to load version");
      }
    },
    [loadProgramDetail, selectedProgramId]
  );

  const createProgram = useCallback(async () => {
    if (!newProgramName.trim()) {
      setError("program name cannot be empty");
      return;
    }
    setCreatingProgram(true);
    setError("");
    try {
      const response = await fetch("/v1/programs", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: newProgramName.trim() })
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const created = (await response.json()) as ProgramDetailResponse;
      await loadPrograms();
      await selectProgram(created.program_id);
      setSection("programs");
      setGraphStatus("program created");
      setNewProgramName(makeDefaultProgramName());
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to create program");
    } finally {
      setCreatingProgram(false);
    }
  }, [loadPrograms, newProgramName, selectProgram]);

  const renameProgram = useCallback(async () => {
    if (!selectedProgramId) {
      return;
    }
    if (!programNameDraft.trim()) {
      setError("program name cannot be empty");
      return;
    }
    setRenamingProgram(true);
    setError("");
    try {
      const response = await fetch(`/v1/programs/${selectedProgramId}`, {
        method: "PATCH",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: programNameDraft.trim() })
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      await loadPrograms();
      await loadProgramDetail(selectedProgramId, selectedVersion);
      setGraphStatus("program renamed");
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to rename program");
    } finally {
      setRenamingProgram(false);
    }
  }, [loadProgramDetail, loadPrograms, programNameDraft, selectedProgramId, selectedVersion]);

  const deleteProgram = useCallback(
    async (programId: string, programName: string) => {
      const confirmed = window.confirm(
        `Delete program "${programName}" and all versions? This cannot be undone.`
      );
      if (!confirmed) {
        return;
      }
      setDeletingProgram(true);
      setError("");
      try {
        const response = await fetch(`/v1/programs/${programId}`, {
          method: "DELETE"
        });
        if (!response.ok) {
          throw new Error(await response.text());
        }
        await loadPrograms();
        setApplyProgramId((current) => (current === programId ? "" : current));
        if (selectedProgramId === programId) {
          setSelectedProgramId(null);
          setSelectedProgram(null);
          setSelectedVersion(null);
          setIsCodeEditMode(false);
          setNodes([]);
          setEdges([]);
          setSource(initialSource);
          setProgramView("list");
          setGraphStatus("program deleted");
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : "failed to delete program");
      } finally {
        setDeletingProgram(false);
      }
    },
    [loadPrograms, selectedProgramId]
  );

  const saveProgramVersion = useCallback(async () => {
    if (!selectedProgramId) {
      setError("select a program first");
      return;
    }
    if (!isCodeEditMode && nodes.length === 0) {
      setError("graph is empty");
      return;
    }
    setSavingVersion(true);
    setError("");
    try {
      const body = isCodeEditMode
        ? { flavor: activeFlavor, flow_synced: false, source }
        : { flavor: activeFlavor, flow_synced: true, ...graphPayload(nodes, edges) };
      const response = await fetch(`/v1/programs/${selectedProgramId}/versions`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body)
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const saved = (await response.json()) as ProgramVersionResponse;
      setSelectedVersion(saved.detail.version);
      setActiveFlavor(normalizeFlavor(saved.detail.flavor));
      setSource(saved.detail.source);
      setIsCodeEditMode(!saved.detail.flow_synced);
      if (saved.detail.flow_synced) {
        const { loadedNodes, skippedNodes } = toFlowNodes(saved.detail.nodes);
        if (saved.detail.nodes.length === 0 || loadedNodes.length === 0) {
          throw new Error(
            saved.detail.nodes.length === 0
              ? "saved version payload contains no nodes"
              : `saved version references ${skippedNodes} unknown block type(s)`
          );
        }
        const loadedEdges = toFlowEdges(saved.detail.edges, loadedNodes);
        hydratingGraphRef.current = true;
        setNodes(loadedNodes);
        setEdges(loadedEdges);
        bumpGraphCanvasRevision();
        clearHydrationTimer();
        hydrationTimerRef.current = window.setTimeout(() => {
          hydratingGraphRef.current = false;
          rfInstance?.fitView({ padding: 0.35 });
          applyPreferredZoom(rfInstance);
          hydrationTimerRef.current = null;
        }, 80);
      } else {
        clearHydrationTimer();
        hydratingGraphRef.current = false;
        setNodes([]);
        setEdges([]);
        bumpGraphCanvasRevision();
      }
      await loadPrograms();
      await loadProgramDetail(selectedProgramId, saved.detail.version).catch(() => {
        // Keep the already-hydrated graph if refresh race fails.
      });
      setGraphStatus(
        saved.detail.flow_synced
          ? `saved version v${saved.detail.version}`
          : `saved version v${saved.detail.version} (code edited)`
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to save version");
    } finally {
      setSavingVersion(false);
    }
  }, [activeFlavor, applyPreferredZoom, bumpGraphCanvasRevision, clearHydrationTimer, edges, isCodeEditMode, loadProgramDetail, loadPrograms, nodes, rfInstance, selectedProgramId, source, toFlowNodes]);

  useEffect(() => {
    setNodes((curr) => applyConnectedInputs(curr, edges));
  }, [edges]);

  useEffect(() => {
    if (isCodeEditMode) {
      return;
    }
    const payload = graphPayload(nodes, edges);
    const controller = new AbortController();
    const timer = setTimeout(async () => {
      setRendering(true);
      try {
        const response = await fetch("/v1/ui/render", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(payload),
          signal: controller.signal
        });
        const text = await response.text();
        if (!response.ok) {
          throw new Error(text || `render failed (${response.status})`);
        }
        const rendered = JSON.parse(text) as UiRenderResponse;
        setSource(rendered.source);
      } catch (err) {
        if ((err as { name?: string }).name !== "AbortError") {
          setError(err instanceof Error ? err.message : "render failed");
        }
      } finally {
        setRendering(false);
      }
    }, 220);
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [edges, isCodeEditMode, nodes]);

  const addNode = useCallback(
    (blockId: string, position?: { x: number; y: number }) => {
      const definition = definitionMap.get(blockId);
      if (!definition) {
        return;
      }
      const nextId = idSequence + 1;
      setIdSequence(nextId);
      const id = `node-${nextId}`;
      const fallback = { x: 130 + ((nextId - 1) % 4) * 56, y: 120 + ((nextId - 1) % 4) * 56 };
      const created: FlowNode = {
        id,
        type: "blockNode",
        position: position ?? fallback,
        data: {
          blockId: definition.id,
          definition,
          values: defaultValues(definition),
          connectedInputs: {},
          onValueChange: updateNodeValue,
          onDelete: removeNode
        }
      };
      setNodes((curr) => [...curr, created]);
    },
    [definitionMap, idSequence, removeNode, updateNodeValue]
  );

  const onNodesChange = useCallback((changes: NodeChange<FlowNode>[]) => {
    if (hydratingGraphRef.current) {
      return;
    }
    setNodes((curr) => applyNodeChanges(changes, curr));
  }, []);

  const onEdgesChange = useCallback((changes: EdgeChange<FlowEdge>[]) => {
    if (hydratingGraphRef.current) {
      return;
    }
    setEdges((curr) => normalizeFlowEdges(applyEdgeChanges(changes, curr)));
  }, []);

  const onConnect = useCallback(
    (connection: Connection) => {
      if (!connection.source || !connection.target || !connection.sourceHandle || !connection.targetHandle) {
        return;
      }
      const sourceNode = nodes.find((node) => node.id === connection.source);
      const targetNode = nodes.find((node) => node.id === connection.target);
      if (!sourceNode || !targetNode) {
        return;
      }
      const sourceHandle = connection.sourceHandle;
      const targetHandle = connection.targetHandle;
      const sourceOutput = sourceNode.data.definition.outputs.find((output) => output.key === sourceHandle);
      if (!sourceOutput) {
        return;
      }
      if (sourceOutput.expr_from_input === null) {
        if (targetHandle !== "__flow" || !targetNode.data.definition.accepts_flow) {
          setError("flow outputs must connect to Flow In");
          return;
        }
      } else {
        const targetInput = targetNode.data.definition.inputs.find((input) => input.key === targetHandle);
        if (!targetInput || !targetInput.connectable) {
          setError("data outputs must connect to connectable input");
          return;
        }
      }

      setEdges((curr) =>
        normalizeFlowEdges(
          addEdge(
            {
              ...connection,
              id: `${connection.source}:${sourceHandle}->${connection.target}:${targetHandle}`,
              data: {
                source_output: sourceHandle,
                target_input: targetHandle
              },
              type: "smoothstep",
              animated: true,
              style: { stroke: "#22d3ee", strokeWidth: 2 }
            },
            curr.filter(
              (edge) => !(edge.target === connection.target && edge.targetHandle === targetHandle)
            )
          )
        )
      );
      setError("");
    },
    [nodes]
  );

  const onFlowInit = useCallback(
    (instance: ReactFlowInstance<FlowNode, FlowEdge>) => {
      setRfInstance(instance);
      applyPreferredZoom(instance);
    },
    [applyPreferredZoom]
  );

  const onFlowMoveEnd = useCallback((viewport: Viewport) => {
    flowZoomRef.current = viewport.zoom;
  }, []);

  const onPaletteDragStart = (event: DragEvent<HTMLDivElement>, blockId: string) => {
    event.dataTransfer.setData("application/x-pd-block", blockId);
    event.dataTransfer.effectAllowed = "move";
  };

  const onCanvasDrop = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    const blockId = event.dataTransfer.getData("application/x-pd-block");
    if (!blockId || !rfInstance) {
      return;
    }
    const position = rfInstance.screenToFlowPosition({ x: event.clientX, y: event.clientY });
    addNode(blockId, position);
  };

  const loadEdgeDetail = useCallback(async (edgeId: string) => {
    const response = await fetch(`/v1/edges/${edgeId}`);
    if (!response.ok) {
      throw new Error(`failed to load edge detail (${response.status})`);
    }
    const detail = (await response.json()) as EdgeDetailResponse;
    setSelectedEdge(detail);
  }, []);

  const selectEdge = useCallback(
    async (edgeId: string) => {
      setSelectedEdgeId(edgeId);
      setEdgeView("detail");
      setError("");
      try {
        await loadEdgeDetail(edgeId);
      } catch (err) {
        setError(err instanceof Error ? err.message : "failed to load edge");
      }
    },
    [loadEdgeDetail]
  );

  useEffect(() => {
    if (!selectedEdgeId) {
      return;
    }
    loadEdgeDetail(selectedEdgeId).catch(() => {
      // ignore silent refresh errors
    });
  }, [loadEdgeDetail, selectedEdgeId]);

  useEffect(() => {
    if (!debugEdgeId && edgeSummaries.length > 0) {
      setDebugEdgeId(edgeSummaries[0].edge_id);
    }
  }, [debugEdgeId, edgeSummaries]);

  const loadDebugSessionDetail = useCallback(async (sessionId: string) => {
    const response = await fetch(`/v1/debug-sessions/${sessionId}`);
    if (!response.ok) {
      throw new Error(`failed to load debug session (${response.status})`);
    }
    const detail = (await response.json()) as DebugSessionDetail;
    setSelectedDebugSession(detail);
    setSelectedDebugSessionId(detail.session_id);
  }, []);

  const selectDebugSession = useCallback(
    async (sessionId: string) => {
      setError("");
      try {
        await loadDebugSessionDetail(sessionId);
        setSection("debug_sessions");
      } catch (err) {
        setError(err instanceof Error ? err.message : "failed to load debug session");
      }
    },
    [loadDebugSessionDetail]
  );

  const createDebugSession = useCallback(async () => {
    if (!debugEdgeId) {
      setError("select an edge for debug session");
      return;
    }
    setDebugCreating(true);
    setError("");
    try {
      const response = await fetch("/v1/debug-sessions", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          edge_id: debugEdgeId,
          header_name: debugHeaderName.trim() || undefined,
          stop_on_entry: true
        })
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const detail = (await response.json()) as DebugSessionDetail;
      await loadDebugSessions();
      await loadDebugSessionDetail(detail.session_id);
      setSection("debug_sessions");
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to create debug session");
    } finally {
      setDebugCreating(false);
    }
  }, [debugEdgeId, debugHeaderName, loadDebugSessionDetail, loadDebugSessions]);

  const stopDebugSession = useCallback(async () => {
    if (!selectedDebugSessionId) {
      return;
    }
    setDebugCommandLoading(true);
    setError("");
    try {
      const response = await fetch(`/v1/debug-sessions/${selectedDebugSessionId}`, {
        method: "DELETE"
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const detail = (await response.json()) as DebugSessionDetail;
      setSelectedDebugSession(detail);
      await loadDebugSessions();
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to stop debug session");
    } finally {
      setDebugCommandLoading(false);
    }
  }, [loadDebugSessions, selectedDebugSessionId]);

  const runDebugCommand = useCallback(
    async (request: DebugCommandRequest, options: RunDebugCommandOptions = {}) => {
      if (!selectedDebugSessionId) {
        return null;
      }
      const { silent = false, refresh = true } = options;
      if (!silent) {
        setDebugCommandLoading(true);
      }
      try {
        const response = await fetch(`/v1/debug-sessions/${selectedDebugSessionId}/command`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(request)
        });
        if (!response.ok) {
          throw new Error(await response.text());
        }
        const result = (await response.json()) as DebugCommandResponse;
        if (refresh) {
          await Promise.all([loadDebugSessions(), loadDebugSessionDetail(selectedDebugSessionId)]);
        }
        return result;
      } catch (err) {
        if (!silent) {
          setError(err instanceof Error ? err.message : "failed to run debugger command");
        }
        return null;
      } finally {
        if (!silent) {
          setDebugCommandLoading(false);
        }
      }
    },
    [loadDebugSessionDetail, loadDebugSessions, selectedDebugSessionId]
  );

  useEffect(() => {
    selectedDebugSessionRef.current = selectedDebugSession;
  }, [selectedDebugSession]);

  useEffect(() => {
    debugCommandLoadingRef.current = debugCommandLoading;
  }, [debugCommandLoading]);

  useEffect(() => {
    runDebugCommandRef.current = runDebugCommand;
  }, [runDebugCommand]);

  const onDebugEditorMount: OnMount = useCallback((editor, monaco) => {
    debugEditorRef.current = editor;
    debugMonacoRef.current = monaco;
    editor.updateOptions({
      readOnly: true,
      glyphMargin: true
    });

    editor.onMouseDown((event) => {
      const session = selectedDebugSessionRef.current;
      if (!session || session.phase !== "attached" || debugCommandLoadingRef.current) {
        return;
      }
      if (
        event.target.type !== monaco.editor.MouseTargetType.GUTTER_GLYPH_MARGIN &&
        event.target.type !== monaco.editor.MouseTargetType.GUTTER_LINE_NUMBERS
      ) {
        return;
      }
      const line = event.target.position?.lineNumber;
      if (!line) {
        return;
      }
      const isBreakpoint = session.breakpoints.includes(line);
      const request: DebugCommandRequest = isBreakpoint ? { kind: "clear_line", line } : { kind: "break_line", line };
      runDebugCommandRef.current?.(request).catch(() => {
        // handled by callback
      });
    });
  }, []);

  useEffect(() => {
    const editor = debugEditorRef.current;
    const monaco = debugMonacoRef.current;
    if (!editor || !monaco) {
      return;
    }
    const model = editor.getModel();
    if (!model) {
      return;
    }

    debugHoverProviderDisposableRef.current?.dispose();
    debugHoverProviderDisposableRef.current = monaco.languages.registerHoverProvider(model.getLanguageId(), {
      provideHover: async (hoverModel, position) => {
        const session = selectedDebugSessionRef.current;
        if (!session || session.phase !== "attached") {
          return null;
        }
        if (hoverModel.uri.toString() !== model.uri.toString()) {
          return null;
        }
        const word = hoverModel.getWordAtPosition(position);
        if (!word || !looksLikeIdentifier(word.word)) {
          return null;
        }

        const cacheKey = `${session.session_id}:${word.word}:${session.current_line ?? 0}`;
        let value = debugHoverCacheRef.current.get(cacheKey);
        if (!value) {
          const result = await runDebugCommandRef.current?.(
            { kind: "print_var", name: word.word },
            { silent: true, refresh: false }
          );
          if (!result) {
            return null;
          }
          value = result.output.trim() || "(no value)";
          debugHoverCacheRef.current.set(cacheKey, value);
        }
        setDebugHoveredVar(word.word);
        setDebugHoverValue(value);

        return {
          range: new monaco.Range(
            position.lineNumber,
            word.startColumn,
            position.lineNumber,
            word.endColumn
          ),
          contents: [
            { value: `**${word.word}**` },
            { value: `\`\`\`text\n${value}\n\`\`\`` }
          ]
        };
      }
    });
    return () => {
      debugHoverProviderDisposableRef.current?.dispose();
      debugHoverProviderDisposableRef.current = null;
    };
  }, [selectedDebugSessionId, selectedDebugSession?.source_code, selectedDebugSession?.source_flavor]);

  useEffect(() => {
    const editor = debugEditorRef.current;
    const monaco = debugMonacoRef.current;
    if (!editor || !monaco || !selectedDebugSession?.source_code) {
      if (editor) {
        debugDecorationIdsRef.current = editor.deltaDecorations(debugDecorationIdsRef.current, []);
      }
      return;
    }

    const decorations: Monaco.editor.IModelDeltaDecoration[] = [];
    const currentLine = selectedDebugSession.current_line;
    if (currentLine && currentLine > 0) {
      decorations.push({
        range: new monaco.Range(currentLine, 1, currentLine, 1),
        options: {
          isWholeLine: true,
          className: "pd-debug-current-line",
          linesDecorationsClassName: "pd-debug-current-line-marker"
        }
      });
    }
    for (const line of selectedDebugSession.breakpoints) {
      decorations.push({
        range: new monaco.Range(line, 1, line, 1),
        options: {
          isWholeLine: true,
          glyphMarginClassName: "pd-debug-breakpoint-glyph",
          glyphMarginHoverMessage: { value: "Breakpoint" }
        }
      });
    }
    debugDecorationIdsRef.current = editor.deltaDecorations(debugDecorationIdsRef.current, decorations);
  }, [selectedDebugSession?.source_code, selectedDebugSession?.current_line, selectedDebugSession?.breakpoints]);

  useEffect(() => {
    debugHoverCacheRef.current.clear();
  }, [selectedDebugSessionId, selectedDebugSession?.current_line]);

  useEffect(() => {
    if (!selectedDebugSessionId) {
      return;
    }
    const timer = setInterval(() => {
      loadDebugSessionDetail(selectedDebugSessionId).catch(() => {
        // ignore background refresh errors
      });
      loadDebugSessions().catch(() => {
        // ignore background refresh errors
      });
    }, 1000);
    return () => clearInterval(timer);
  }, [loadDebugSessionDetail, loadDebugSessions, selectedDebugSessionId]);

  useEffect(() => {
    setDebugHoveredVar("");
    setDebugHoverValue("");
  }, [selectedDebugSessionId, selectedDebugSession?.current_line]);

  const applyProgramToEdge = useCallback(async () => {
    if (!selectedEdgeId || !applyProgramId) {
      setError("select edge and program");
      return;
    }
    const selectedProgramForApply = programs.find((program) => program.program_id === applyProgramId) ?? null;
    if (!selectedProgramForApply) {
      setError("selected program was not found");
      return;
    }
    if (selectedProgramForApply.latest_version === 0) {
      setError("selected program has no versions; save a version in Programs before applying");
      return;
    }
    setApplyLoading(true);
    setApplyStatus("");
    setError("");
    try {
      const body: { program_id: string; version?: number } = {
        program_id: applyProgramId
      };
      if (applyVersion !== "latest") {
        const parsed = Number.parseInt(applyVersion, 10);
        if (!Number.isNaN(parsed)) {
          body.version = parsed;
        }
      }

      const response = await fetch(`/v1/edges/${selectedEdgeId}/commands/apply-program-version`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body)
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const queued = (await response.json()) as QueueResponse;
      setApplyStatus(`queued ${queued.command_id}, pending=${queued.pending_commands}`);
      await loadEdges();
      await loadEdgeDetail(selectedEdgeId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to apply program");
    } finally {
      setApplyLoading(false);
    }
  }, [applyProgramId, applyVersion, loadEdgeDetail, loadEdges, programs, selectedEdgeId]);

  const selectedApplyProgram = useMemo(
    () => programs.find((program) => program.program_id === applyProgramId) ?? null,
    [applyProgramId, programs]
  );

  const selectedNodeCount = nodes.filter((node) => node.selected).length;
  const selectedEdgeCount = edges.filter((edge) => edge.selected).length;
  const filteredEdges = useMemo(() => {
    const keyword = edgeSearch.trim().toLowerCase();
    if (!keyword) {
      return edgeSummaries;
    }
    return edgeSummaries.filter((edge) => edge.edge_name.toLowerCase().includes(keyword));
  }, [edgeSearch, edgeSummaries]);

  const edgeStats = useMemo(() => {
    let healthy = 0;
    let degraded = 0;
    let pending = 0;
    for (const edge of edgeSummaries) {
      const health = edgeHealth(edge);
      if (health === "healthy") {
        healthy += 1;
      } else if (health === "degraded") {
        degraded += 1;
      }
      pending += edge.pending_commands;
    }
    return {
      total: edgeSummaries.length,
      healthy,
      degraded,
      pending
    };
  }, [edgeSummaries]);
  const filteredPrograms = useMemo(() => {
    const keyword = programSearch.trim().toLowerCase();
    if (!keyword) {
      return programs;
    }
    return programs.filter((program) => program.name.toLowerCase().includes(keyword));
  }, [programSearch, programs]);

  const debugSessionsSorted = useMemo(() => {
    return [...debugSessions].sort((lhs, rhs) => rhs.updated_unix_ms - lhs.updated_unix_ms);
  }, [debugSessions]);

  const debugStartDisabledReason = useMemo(() => {
    if (!debugEdgeId) {
      return "Select an edge first.";
    }
    const selectedEdgeSummary = edgeSummaries.find((edge) => edge.edge_id === debugEdgeId);
    if (!selectedEdgeSummary) {
      return "Selected edge is not available.";
    }
    if (!selectedEdgeSummary.last_telemetry) {
      return "No telemetry yet for this edge. Wait for it to poll the controller.";
    }
    if (!selectedEdgeSummary.last_telemetry.program_loaded) {
      return "This edge has no loaded program yet. Apply a program before starting debug.";
    }
    return null;
  }, [debugEdgeId, edgeSummaries]);
  return (
    <div className="flex min-h-screen bg-background text-foreground">
      <NavBar
        section={section}
        onSelectEdges={() => {
          setSection("edges");
          setEdgeView("list");
        }}
        onSelectPrograms={() => {
          setSection("programs");
          setProgramView("list");
        }}
        onSelectDebugSessions={() => setSection("debug_sessions")}
      />

      <main className="min-w-0 flex-1 bg-gradient-to-br from-slate-50 via-white to-sky-50 p-4 lg:p-6">
        {error ? (
          <Card className="mb-4 border-red-300 bg-red-50">
            <CardContent className="p-3 text-sm text-red-700">{error}</CardContent>
          </Card>
        ) : null}

        {section === "edges" ? (
          edgeView === "list" ? (
            <EdgeListView
              edgeStats={edgeStats}
              edgeSearch={edgeSearch}
              onEdgeSearchChange={setEdgeSearch}
              filteredEdges={filteredEdges}
              onSelectEdge={selectEdge}
            />
          ) : (
            <EdgeDetailView
              selectedEdge={selectedEdge}
              onBack={() => setEdgeView("list")}
              programs={programs}
              applyProgramId={applyProgramId}
              onApplyProgramChange={(programId) => {
                setApplyProgramId(programId);
                setApplyVersion("latest");
              }}
              applyVersion={applyVersion}
              onApplyVersionChange={setApplyVersion}
              selectedApplyProgram={selectedApplyProgram}
              applyLoading={applyLoading}
              applyStatus={applyStatus}
              onApplyProgram={applyProgramToEdge}
            />
          )
        ) : section === "programs" ? (
          programView === "list" ? (
            <ProgramListView
              newProgramName={newProgramName}
              onNewProgramNameChange={setNewProgramName}
              creatingProgram={creatingProgram}
              onCreateProgram={createProgram}
              programSearch={programSearch}
              onProgramSearchChange={setProgramSearch}
              filteredPrograms={filteredPrograms}
              onSelectProgram={selectProgram}
            />
          ) : (
            <ProgramDetailView
              selectedProgram={selectedProgram}
              programNameDraft={programNameDraft}
              onProgramNameDraftChange={setProgramNameDraft}
              selectedVersion={selectedVersion}
              onSelectVersion={selectProgramVersion}
              renamingProgram={renamingProgram}
              onRenameProgram={renameProgram}
              deletingProgram={deletingProgram}
              onDeleteProgram={() => {
                if (!selectedProgram) {
                  return;
                }
                deleteProgram(selectedProgram.program_id, selectedProgram.name).catch(() => {
                  // handled in callback
                });
              }}
              savingVersion={savingVersion}
              canSaveVersion={isCodeEditMode || nodes.length > 0}
              onSaveVersion={saveProgramVersion}
              graphStatus={graphStatus}
              onBackToPrograms={() => setProgramView("list")}
              isCodeEditMode={isCodeEditMode}
              onExitCodeEditMode={() => setIsCodeEditMode(false)}
              onEnterCodeEditMode={() => setIsCodeEditMode(true)}
              source={source}
              activeFlavor={activeFlavor}
              rendering={rendering}
              onFlavorChange={setActiveFlavor}
              onSourceChange={updateSourceText}
              selectedProgramId={selectedProgramId}
              graphCanvasRevision={graphCanvasRevision}
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              onInit={onFlowInit}
              onMoveEnd={onFlowMoveEnd}
              onCanvasDrop={onCanvasDrop}
              selectedNodeCount={selectedNodeCount}
              selectedEdgeCount={selectedEdgeCount}
              paletteMinimized={paletteMinimized}
              onTogglePaletteMinimized={() => setPaletteMinimized((value) => !value)}
              codePanelMinimized={codePanelMinimized}
              onToggleCodePanelMinimized={() => setCodePanelMinimized((value) => !value)}
              definitions={filteredDefinitions}
              search={search}
              onSearchChange={setSearch}
              onPaletteDragStart={onPaletteDragStart}
              onAddNode={addNode}
            />
          )
        ) : (
          <DebugSessionsView
            debugEdgeId={debugEdgeId}
            onDebugEdgeIdChange={setDebugEdgeId}
            edgeSummaries={edgeSummaries}
            debugHeaderName={debugHeaderName}
            onDebugHeaderNameChange={setDebugHeaderName}
            onCreateDebugSession={createDebugSession}
            debugCreating={debugCreating}
            startDisabledReason={debugStartDisabledReason}
            debugSessionsSorted={debugSessionsSorted}
            selectedDebugSessionId={selectedDebugSessionId}
            onSelectDebugSession={selectDebugSession}
            selectedDebugSession={selectedDebugSession}
            runDebugCommand={runDebugCommand}
            onStopDebugSession={stopDebugSession}
            debugCommandLoading={debugCommandLoading}
            onDebugEditorMount={onDebugEditorMount}
            debugHoveredVar={debugHoveredVar}
            debugHoverValue={debugHoverValue}
          />
        )}
      </main>
    </div>
  );
}
