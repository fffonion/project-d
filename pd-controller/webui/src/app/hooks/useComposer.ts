import { useCallback, useEffect, useMemo, useRef, useState, type DragEvent } from "react";
import {
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  type Connection,
  type EdgeChange,
  type NodeChange,
  type ReactFlowInstance,
  type Viewport
} from "@xyflow/react";

import {
  applyConnectedInputs,
  defaultValues,
  graphPayload,
  normalizeFlowEdges
} from "@/app/helpers";
import type {
  FlowEdge,
  FlowNode,
  SourceFlavor,
  UiBlockDefinition,
  UiBlocksResponse,
  UiGraphNode,
  UiRenderResponse,
  UiSourceBundle
} from "@/app/types";
import { initialSource } from "@/app/types";

type UseComposerArgs = {
  onError: (message: string) => void;
};

export type ComposerProgramApi = {
  activeFlavor: SourceFlavor;
  edges: FlowEdge[];
  isCodeEditMode: boolean;
  nodes: FlowNode[];
  source: UiSourceBundle;
  setActiveFlavor: (value: SourceFlavor) => void;
  setSource: (value: UiSourceBundle) => void;
  setIsCodeEditMode: (value: boolean) => void;
  setGraphStatus: (value: string) => void;
  toFlowNodes: (graphNodes: UiGraphNode[]) => { loadedNodes: FlowNode[]; skippedNodes: number };
  hydrateGraph: (loadedNodes: FlowNode[], loadedEdges: FlowEdge[], isCurrent?: () => boolean) => void;
  clearHydrationState: () => void;
  clearGraphForCodeVersion: () => void;
  resetComposerToDraft: () => void;
};

export function useComposer({ onError }: UseComposerArgs) {
  const [definitions, setDefinitions] = useState<UiBlockDefinition[]>([]);
  const [search, setSearch] = useState("");
  const [nodes, setNodes] = useState<FlowNode[]>([]);
  const [edges, setEdges] = useState<FlowEdge[]>([]);
  const [source, setSourceState] = useState<UiSourceBundle>(initialSource);
  const [activeFlavorState, setActiveFlavorState] = useState<SourceFlavor>("rustscript");
  const [rendering, setRendering] = useState(false);
  const [rfInstance, setRfInstance] = useState<ReactFlowInstance<FlowNode, FlowEdge> | null>(null);
  const [idSequence, setIdSequence] = useState(0);
  const [graphStatusState, setGraphStatusState] = useState("");
  const [graphCanvasRevision, setGraphCanvasRevision] = useState(0);
  const [paletteMinimized, setPaletteMinimized] = useState(false);
  const [codePanelMinimized, setCodePanelMinimized] = useState(false);
  const [isCodeEditModeState, setIsCodeEditModeState] = useState(false);

  const hydratingGraphRef = useRef(false);
  const flowZoomRef = useRef(0.5);
  const hydrationTimerRef = useRef<number | null>(null);

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

  const clearHydrationState = useCallback(() => {
    clearHydrationTimer();
    hydratingGraphRef.current = false;
  }, [clearHydrationTimer]);

  const clearGraphForCodeVersion = useCallback(() => {
    clearHydrationState();
    setNodes([]);
    setEdges([]);
    bumpGraphCanvasRevision();
  }, [bumpGraphCanvasRevision, clearHydrationState]);

  const resetComposerToDraft = useCallback(() => {
    clearGraphForCodeVersion();
    setSourceState(initialSource);
    setIsCodeEditModeState(false);
  }, [clearGraphForCodeVersion]);

  const hydrateGraph = useCallback(
    (loadedNodes: FlowNode[], loadedEdges: FlowEdge[], isCurrent?: () => boolean) => {
      hydratingGraphRef.current = true;
      setNodes(loadedNodes);
      setEdges(loadedEdges);
      bumpGraphCanvasRevision();
      clearHydrationTimer();
      hydrationTimerRef.current = window.setTimeout(() => {
        if (isCurrent && !isCurrent()) {
          return;
        }
        hydratingGraphRef.current = false;
        rfInstance?.fitView({ padding: 0.35 });
        applyPreferredZoom(rfInstance);
        hydrationTimerRef.current = null;
      }, 80);
    },
    [applyPreferredZoom, bumpGraphCanvasRevision, clearHydrationTimer, rfInstance]
  );

  useEffect(() => {
    return () => {
      clearHydrationState();
    };
  }, [clearHydrationState]);

  const loadBlocks = useCallback(async () => {
    const response = await fetch("/v1/ui/blocks");
    if (!response.ok) {
      throw new Error(`failed to load blocks (${response.status})`);
    }
    const data = (await response.json()) as UiBlocksResponse;
    setDefinitions(data.blocks);
  }, []);

  const updateSourceText = useCallback((flavor: SourceFlavor, value: string) => {
    setSourceState((curr) => ({ ...curr, [flavor]: value }));
  }, []);

  const setSource = useCallback((value: UiSourceBundle) => {
    setSourceState(value);
  }, []);

  const setActiveFlavor = useCallback((value: SourceFlavor) => {
    setActiveFlavorState(value);
  }, []);

  const setIsCodeEditMode = useCallback((value: boolean) => {
    setIsCodeEditModeState(value);
  }, []);

  const setGraphStatus = useCallback((value: string) => {
    setGraphStatusState(value);
  }, []);

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

  useEffect(() => {
    setNodes((curr) => applyConnectedInputs(curr, edges));
  }, [edges]);

  useEffect(() => {
    if (isCodeEditModeState) {
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
        setSourceState(rendered.source);
      } catch (err) {
        if ((err as { name?: string }).name !== "AbortError") {
          onError(err instanceof Error ? err.message : "render failed");
        }
      } finally {
        setRendering(false);
      }
    }, 220);
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [edges, isCodeEditModeState, nodes, onError]);

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
          onError("flow outputs must connect to Flow In");
          return;
        }
      } else {
        const targetInput = targetNode.data.definition.inputs.find((input) => input.key === targetHandle);
        if (!targetInput || !targetInput.connectable) {
          onError("data outputs must connect to connectable input");
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
              type: "bezier",
              animated: true,
              style: { stroke: "#22d3ee", strokeWidth: 2 }
            },
            curr.filter((edge) => !(edge.target === connection.target && edge.targetHandle === targetHandle))
          )
        )
      );
      onError("");
    },
    [nodes, onError]
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

  const onPaletteDragStart = useCallback((event: DragEvent<HTMLDivElement>, blockId: string) => {
    event.dataTransfer.setData("application/x-pd-block", blockId);
    event.dataTransfer.effectAllowed = "move";
  }, []);

  const onCanvasDrop = useCallback(
    (event: DragEvent<HTMLDivElement>) => {
      event.preventDefault();
      const blockId = event.dataTransfer.getData("application/x-pd-block");
      if (!blockId || !rfInstance) {
        return;
      }
      const position = rfInstance.screenToFlowPosition({ x: event.clientX, y: event.clientY });
      addNode(blockId, position);
    },
    [addNode, rfInstance]
  );

  const selectedNodeCount = nodes.filter((node) => node.selected).length;
  const selectedEdgeCount = edges.filter((edge) => edge.selected).length;

  return {
    definitions,
    filteredDefinitions,
    search,
    setSearch,
    nodes,
    edges,
    source,
    activeFlavor: activeFlavorState,
    rendering,
    graphStatus: graphStatusState,
    graphCanvasRevision,
    paletteMinimized,
    setPaletteMinimized,
    codePanelMinimized,
    setCodePanelMinimized,
    isCodeEditMode: isCodeEditModeState,
    setIsCodeEditMode,
    setSource,
    setActiveFlavor,
    setGraphStatus,
    selectedNodeCount,
    selectedEdgeCount,
    loadBlocks,
    updateSourceText,
    addNode,
    onNodesChange,
    onEdgesChange,
    onConnect,
    onFlowInit,
    onFlowMoveEnd,
    onPaletteDragStart,
    onCanvasDrop,
    toFlowNodes,
    hydrateGraph,
    clearHydrationState,
    clearGraphForCodeVersion,
    resetComposerToDraft
  };
}
