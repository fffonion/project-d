import { useCallback, useEffect, useMemo, useState, type DragEvent } from "react";
import {
  Activity,
  ArrowLeft,
  ChevronRight,
  Circle,
  FileCode2,
  Maximize2,
  Minimize2,
  Plus,
  Save,
  Server,
  Trash2,
  WandSparkles
} from "lucide-react";
import {
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  Background,
  type Connection,
  Controls,
  type Edge,
  type EdgeChange,
  Handle,
  MiniMap,
  type Node,
  type NodeChange,
  type NodeProps,
  Position,
  ReactFlow,
  type ReactFlowInstance,
  type NodeTypes
} from "@xyflow/react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneLight } from "react-syntax-highlighter/dist/esm/styles/prism";
import "@xyflow/react/dist/style.css";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

type Section = "edges" | "programs";
type SourceFlavor = "rustscript" | "javascript" | "lua" | "scheme";
type UiInputType = "text" | "number";

type UiBlockInput = {
  key: string;
  label: string;
  input_type: UiInputType;
  default_value: string;
  placeholder: string;
  connectable: boolean;
};

type UiBlockOutput = {
  key: string;
  label: string;
  expr_from_input: string | null;
};

type UiBlockDefinition = {
  id: string;
  title: string;
  category: string;
  description: string;
  inputs: UiBlockInput[];
  outputs: UiBlockOutput[];
  accepts_flow: boolean;
};

type UiBlocksResponse = { blocks: UiBlockDefinition[] };

type UiSourceBundle = {
  rustscript: string;
  javascript: string;
  lua: string;
  scheme: string;
};

type UiRenderResponse = { source: UiSourceBundle };

type UiGraphNode = {
  id: string;
  block_id: string;
  values: Record<string, string>;
  position?: { x: number; y: number };
};

type UiGraphEdge = {
  source: string;
  source_output: string;
  target: string;
  target_input: string;
};

type ProgramSummary = {
  program_id: string;
  name: string;
  latest_version: number;
  versions: number;
  created_unix_ms: number;
  updated_unix_ms: number;
};

type ProgramVersionSummary = {
  version: number;
  created_unix_ms: number;
  flavor: string;
};

type ProgramListResponse = { programs: ProgramSummary[] };

type ProgramDetailResponse = {
  program_id: string;
  name: string;
  latest_version: number;
  created_unix_ms: number;
  updated_unix_ms: number;
  versions: ProgramVersionSummary[];
};

type ProgramVersionDetail = {
  version: number;
  created_unix_ms: number;
  flavor: string;
  nodes: UiGraphNode[];
  edges: UiGraphEdge[];
  source: UiSourceBundle;
};

type ProgramVersionResponse = {
  program_id: string;
  name: string;
  detail: ProgramVersionDetail;
};

type EdgeSummary = {
  edge_id: string;
  edge_name: string;
  sync_status: "synced" | "out_of_sync" | "not_synced" | string;
  last_seen_unix_ms: number | null;
  pending_commands: number;
  recent_results: number;
  applied_program: AppliedProgramRef | null;
  last_poll_unix_ms: number | null;
  last_result_unix_ms: number | null;
  total_polls: number;
  total_results: number;
  last_telemetry: TelemetrySnapshot | null;
};

type AppliedProgramRef = {
  program_id: string;
  name: string;
  version: number;
};

type EdgeListResponse = { edges: EdgeSummary[] };

type TelemetrySnapshot = {
  uptime_seconds: number;
  program_loaded: boolean;
  debug_session_active: boolean;
  data_requests_total: number;
  vm_execution_errors_total: number;
  program_apply_success_total: number;
  program_apply_failure_total: number;
  control_rpc_polls_success_total: number;
  control_rpc_polls_error_total: number;
  control_rpc_results_success_total: number;
  control_rpc_results_error_total: number;
};

type EdgeDetailResponse = {
  summary: EdgeSummary;
  pending_command_types: string[];
  traffic_series: EdgeTrafficPoint[];
};

type EdgeTrafficPoint = {
  unix_ms: number;
  requests: number;
  status_2xx: number;
  status_3xx: number;
  status_4xx: number;
  status_5xx: number;
};

type QueueResponse = {
  command_id: string;
  pending_commands: number;
};

type FlowNodeData = {
  blockId: string;
  definition: UiBlockDefinition;
  values: Record<string, string>;
  connectedInputs: Record<string, boolean>;
  onValueChange: (nodeId: string, key: string, value: string) => void;
  onDelete: (nodeId: string) => void;
};

type FlowNode = Node<FlowNodeData, "blockNode">;
type FlowEdgeData = {
  source_output: string;
  target_input: string;
};
type FlowEdge = Edge<FlowEdgeData>;

function normalizeFlowEdges(edges: FlowEdge[]): FlowEdge[] {
  const normalized: FlowEdge[] = [];
  for (const edge of edges) {
    const sourceHandle = edge.sourceHandle ?? edge.data?.source_output ?? null;
    const targetHandle = edge.targetHandle ?? edge.data?.target_input ?? null;
    if (!edge.source || !edge.target || !sourceHandle || !targetHandle) {
      continue;
    }
    normalized.push({
      ...edge,
      sourceHandle,
      targetHandle,
      data: {
        source_output: sourceHandle,
        target_input: targetHandle
      }
    });
  }
  return normalized;
}

const initialSource: UiSourceBundle = {
  rustscript: "use vm;\n",
  javascript: "import * as vm from \"vm\";\n",
  lua: "local vm = require(\"vm\")\n",
  scheme: "(require (prefix-in vm. \"vm\"))\n"
};

function defaultValues(definition: UiBlockDefinition): Record<string, string> {
  const values: Record<string, string> = {};
  for (const input of definition.inputs) {
    values[input.key] = input.default_value;
  }
  return values;
}

function graphPayload(nodes: FlowNode[], edges: FlowEdge[]) {
  const mappedEdges = normalizeFlowEdges(edges)
    .map((edge) => {
      const sourceOutput = edge.sourceHandle;
      const targetInput = edge.targetHandle;
      if (!sourceOutput || !targetInput) {
        return null;
      }
      return {
        source: edge.source,
        source_output: sourceOutput,
        target: edge.target,
        target_input: targetInput
      };
    })
    .filter((edge): edge is UiGraphEdge => edge !== null);

  return {
    nodes: nodes.map((node) => ({
      id: node.id,
      block_id: node.data.blockId,
      values: node.data.values,
      position: {
        x: node.position.x,
        y: node.position.y
      }
    })),
    edges: mappedEdges
  };
}

function applyConnectedInputs(nodes: FlowNode[], edges: FlowEdge[]): FlowNode[] {
  const connectionMap: Record<string, Record<string, boolean>> = {};
  for (const edge of normalizeFlowEdges(edges)) {
    const targetHandle = edge.targetHandle;
    if (!targetHandle) {
      continue;
    }
    if (!connectionMap[edge.target]) {
      connectionMap[edge.target] = {};
    }
    connectionMap[edge.target][targetHandle] = true;
  }
  return nodes.map((node) => ({
    ...node,
    data: {
      ...node.data,
      connectedInputs: connectionMap[node.id] ?? {}
    }
  }));
}

function toFlowEdges(edges: UiGraphEdge[]): FlowEdge[] {
  return normalizeFlowEdges(edges.map((edge) => ({
    id: `${edge.source}:${edge.source_output}->${edge.target}:${edge.target_input}`,
    source: edge.source,
    sourceHandle: edge.source_output,
    target: edge.target,
    targetHandle: edge.target_input,
    data: { source_output: edge.source_output, target_input: edge.target_input },
    type: "smoothstep",
    animated: true,
    style: { stroke: "#22d3ee", strokeWidth: 2 }
  })));
}

function formatUnixMs(value: number | null | undefined): string {
  if (!value) {
    return "-";
  }
  return new Date(value).toLocaleString();
}

function formatNumber(value: number): string {
  return Intl.NumberFormat().format(value);
}

function edgeHealth(summary: EdgeSummary): "healthy" | "degraded" | "idle" {
  if (!summary.last_telemetry) {
    return "idle";
  }
  const telemetry = summary.last_telemetry;
  if (telemetry.control_rpc_polls_error_total > 0 || telemetry.control_rpc_results_error_total > 0) {
    return "degraded";
  }
  return "healthy";
}

function edgeHealthClasses(summary: EdgeSummary): string {
  const health = edgeHealth(summary);
  if (health === "healthy") {
    return "text-emerald-600";
  }
  if (health === "degraded") {
    return "text-amber-600";
  }
  return "text-slate-500";
}

function syncStatusClasses(status: EdgeSummary["sync_status"]): string {
  if (status === "synced") {
    return "text-emerald-600";
  }
  if (status === "out_of_sync") {
    return "text-amber-600";
  }
  return "text-slate-500";
}

function normalizeFlavor(value: string): SourceFlavor {
  const lower = value.trim().toLowerCase();
  if (lower === "javascript" || lower === "js") {
    return "javascript";
  }
  if (lower === "lua") {
    return "lua";
  }
  if (lower === "scheme" || lower === "scm") {
    return "scheme";
  }
  return "rustscript";
}

function HighlightedCode({ flavor, source }: { flavor: SourceFlavor; source: UiSourceBundle }) {
  const language =
    flavor === "javascript" ? "javascript" : flavor === "lua" ? "lua" : flavor === "scheme" ? "scheme" : "rust";
  const code = source[flavor];

  return (
    <div className="max-h-[520px] overflow-auto rounded-md border border-border">
      <SyntaxHighlighter
        language={language}
        style={oneLight}
        customStyle={{ margin: 0, minHeight: "520px", fontSize: "12px", background: "transparent" }}
        showLineNumbers
        wrapLongLines
      >
        {code}
      </SyntaxHighlighter>
    </div>
  );
}

function LineChart({
  points,
  valueFor,
  stroke,
  emptyLabel
}: {
  points: EdgeTrafficPoint[];
  valueFor: (point: EdgeTrafficPoint) => number;
  stroke: string;
  emptyLabel: string;
}) {
  if (points.length === 0) {
    return <div className="h-[160px] rounded-md border bg-background/70 p-3 text-sm text-muted-foreground">{emptyLabel}</div>;
  }

  const width = 520;
  const height = 160;
  const maxY = Math.max(...points.map((point) => valueFor(point)), 1);
  const step = points.length > 1 ? width / (points.length - 1) : 0;
  const path = points
    .map((point, index) => {
      const x = index * step;
      const value = valueFor(point);
      const y = height - (value / maxY) * (height - 10) - 5;
      return `${index === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(" ");

  return (
    <div className="rounded-md border bg-background/70 p-3">
      <svg viewBox={`0 0 ${width} ${height}`} className="h-[160px] w-full">
        <path d={path} fill="none" stroke={stroke} strokeWidth={2.5} />
      </svg>
      <div className="mt-2 text-xs text-muted-foreground">
        latest={valueFor(points[points.length - 1])} max={maxY}
      </div>
    </div>
  );
}

function MultiLineChart({
  points,
  series,
  emptyLabel,
  hideZeroSeries = false
}: {
  points: EdgeTrafficPoint[];
  series: Array<{ key: string; stroke: string; valueFor: (point: EdgeTrafficPoint) => number }>;
  emptyLabel: string;
  hideZeroSeries?: boolean;
}) {
  const visibleSeries = hideZeroSeries
    ? series.filter((item) => points.some((point) => item.valueFor(point) > 0))
    : series;
  if (points.length === 0 || visibleSeries.length === 0) {
    return (
      <div className="rounded-md border bg-background/70 p-3">
        <svg viewBox="0 0 520 160" className="h-[160px] w-full" aria-label={emptyLabel}>
          <path d="M 0 80 L 520 80" fill="none" stroke="#cbd5e1" strokeWidth={2} strokeOpacity={0.75} />
        </svg>
        <div className="mt-2 h-4" aria-hidden="true" />
      </div>
    );
  }

  const width = 520;
  const height = 160;
  const maxY = Math.max(
    ...points.flatMap((point) => visibleSeries.map((item) => item.valueFor(point))),
    1
  );
  const step = points.length > 1 ? width / (points.length - 1) : 0;
  const lines = visibleSeries.map((item) => {
    const path = points
      .map((point, index) => {
        const x = index * step;
        const value = item.valueFor(point);
        const y = height - (value / maxY) * (height - 12) - 6;
        return `${index === 0 ? "M" : "L"} ${x.toFixed(1)} ${y.toFixed(1)}`;
      })
      .join(" ");
    return { ...item, path };
  });

  return (
    <div className="rounded-md border bg-background/70 p-3">
      <svg viewBox={`0 0 ${width} ${height}`} className="h-[160px] w-full">
        {lines.map((line) => (
          <path key={line.key} d={line.path} fill="none" stroke={line.stroke} strokeWidth={2.2} />
        ))}
      </svg>
      <div className="mt-2 flex flex-wrap gap-3 text-xs text-muted-foreground">
        {lines.map((line) => (
          <div key={`${line.key}-legend`} className="inline-flex items-center gap-1">
            <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: line.stroke }} />
            {line.key}
          </div>
        ))}
      </div>
    </div>
  );
}

function BlockNode({ id, data }: NodeProps<FlowNode>) {
  return (
    <div className="min-w-[280px] rounded-lg border border-slate-700 bg-slate-900 text-slate-100 shadow-xl">
      <div className="flex items-center justify-between border-b border-slate-700 px-3 py-2">
        <div className="text-sm font-semibold">{data.definition.title}</div>
        <Badge className="border-slate-600 bg-slate-800 text-slate-200">{data.definition.category}</Badge>
      </div>

      <div className="space-y-2 px-3 py-3">
        {data.definition.accepts_flow ? (
          <div className="relative rounded-md bg-slate-800/70 p-2">
            <div className="text-xs text-slate-300">Flow In</div>
            <Handle
              type="target"
              id="__flow"
              position={Position.Left}
              className="!h-3 !w-3 !border-2 !border-slate-950 !bg-emerald-400"
              style={{ left: -8, top: "50%", transform: "translateY(-50%)" }}
            />
          </div>
        ) : null}

        {data.definition.inputs.map((input) => {
          const connected = data.connectedInputs[input.key] ?? false;
          return (
            <div key={`${id}-${input.key}`} className="relative space-y-1 rounded-md bg-slate-800/70 p-2">
              <Label htmlFor={`${id}-${input.key}`} className="text-xs text-slate-300">
                {input.label}
              </Label>
              <Input
                id={`${id}-${input.key}`}
                type={input.input_type === "number" ? "number" : "text"}
                value={data.values[input.key] ?? ""}
                disabled={connected}
                className="h-8 border-slate-600 bg-slate-900 text-xs text-slate-100"
                placeholder={input.placeholder}
                onChange={(event) => data.onValueChange(id, input.key, event.target.value)}
              />
              {input.connectable ? (
                <Handle
                  type="target"
                  id={input.key}
                  position={Position.Left}
                  className="!h-3 !w-3 !border-2 !border-slate-950 !bg-cyan-400"
                  style={{ left: -8, top: "50%", transform: "translateY(-50%)" }}
                />
              ) : null}
            </div>
          );
        })}
      </div>

      {data.definition.outputs.length > 0 ? (
        <div className="border-t border-slate-700 px-3 py-2">
          <div className="text-[11px] uppercase tracking-wide text-slate-400">Outputs</div>
          <div className="mt-1 space-y-1">
            {data.definition.outputs.map((output) => (
              <div key={`${id}-${output.key}`} className="relative rounded-md bg-slate-800/70 px-2 py-1 text-xs text-slate-200">
                {output.label}
                <Handle
                  type="source"
                  id={output.key}
                  position={Position.Right}
                  className={`!h-3 !w-3 !border-2 !border-slate-950 ${
                    output.expr_from_input ? "!bg-amber-400" : "!bg-emerald-400"
                  }`}
                  style={{ right: -8, top: "50%", transform: "translateY(-50%)" }}
                />
              </div>
            ))}
          </div>
        </div>
      ) : null}

      <div className="flex justify-end border-t border-slate-700 px-3 py-2">
        <Button size="sm" variant="ghost" onClick={() => data.onDelete(id)} className="h-7 px-2 text-slate-200 hover:bg-slate-800">
          <Trash2 className="h-3.5 w-3.5" />
        </Button>
      </div>
    </div>
  );
}

const nodeTypes = {
  blockNode: BlockNode
} satisfies NodeTypes;

function NavButton({
  active,
  icon,
  label,
  onClick
}: {
  active: boolean;
  icon: React.ReactNode;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm ${
        active ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted"
      }`}
    >
      {icon}
      <span>{label}</span>
    </button>
  );
}

export default function App() {
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
  const [paletteMinimized, setPaletteMinimized] = useState(false);
  const [codePanelMinimized, setCodePanelMinimized] = useState(false);

  const [programs, setPrograms] = useState<ProgramSummary[]>([]);
  const [selectedProgramId, setSelectedProgramId] = useState<string | null>(null);
  const [selectedProgram, setSelectedProgram] = useState<ProgramDetailResponse | null>(null);
  const [selectedVersion, setSelectedVersion] = useState<number | null>(null);
  const [programView, setProgramView] = useState<"list" | "composer">("list");
  const [programSearch, setProgramSearch] = useState("");
  const [programNameDraft, setProgramNameDraft] = useState("");
  const [newProgramName, setNewProgramName] = useState("my-workflow");
  const [creatingProgram, setCreatingProgram] = useState(false);
  const [savingVersion, setSavingVersion] = useState(false);
  const [renamingProgram, setRenamingProgram] = useState(false);

  const [edgeSummaries, setEdgeSummaries] = useState<EdgeSummary[]>([]);
  const [edgeView, setEdgeView] = useState<"list" | "detail">("list");
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const [selectedEdge, setSelectedEdge] = useState<EdgeDetailResponse | null>(null);
  const [edgeSearch, setEdgeSearch] = useState("");
  const [applyProgramId, setApplyProgramId] = useState<string>("");
  const [applyVersion, setApplyVersion] = useState<string>("latest");
  const [applyLoading, setApplyLoading] = useState(false);
  const [applyStatus, setApplyStatus] = useState("");

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

  useEffect(() => {
    Promise.all([loadBlocks(), loadPrograms(), loadEdges()]).catch((err) => {
      setError(err instanceof Error ? err.message : "failed to initialize ui");
    });
  }, [loadBlocks, loadEdges, loadPrograms]);

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
      for (let index = 0; index < graphNodes.length; index += 1) {
        const graphNode = graphNodes[index];
        const definition = definitionMap.get(graphNode.block_id);
        if (!definition) {
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
      return loadedNodes;
    },
    [definitionMap, removeNode, updateNodeValue]
  );

  const loadProgramDetail = useCallback(
    async (programId: string, preferredVersion?: number | null) => {
      const detailResp = await fetch(`/v1/programs/${programId}`);
      if (!detailResp.ok) {
        throw new Error(`failed to load program (${detailResp.status})`);
      }
      const detail = (await detailResp.json()) as ProgramDetailResponse;
      setSelectedProgram(detail);
      setProgramNameDraft(detail.name);

      if (detail.versions.length === 0) {
        setSelectedVersion(0);
        setNodes([]);
        setEdges([]);
        setSource(initialSource);
        setGraphStatus("draft v0");
        return;
      }

      const versionToLoad =
        preferredVersion && detail.versions.some((item) => item.version === preferredVersion)
          ? preferredVersion
          : detail.versions[detail.versions.length - 1].version;

      const versionResp = await fetch(`/v1/programs/${programId}/versions/${versionToLoad}`);
      if (!versionResp.ok) {
        throw new Error(`failed to load program version (${versionResp.status})`);
      }
      const version = (await versionResp.json()) as ProgramVersionResponse;
      setSelectedVersion(version.detail.version);
      setActiveFlavor(normalizeFlavor(version.detail.flavor));
      setSource(version.detail.source);
      setNodes(toFlowNodes(version.detail.nodes));
      setEdges(toFlowEdges(version.detail.edges));
      setTimeout(() => {
        rfInstance?.fitView({ padding: 0.24 });
      }, 80);
    },
    [rfInstance, toFlowNodes]
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

  const saveProgramVersion = useCallback(async () => {
    if (!selectedProgramId) {
      setError("select a program first");
      return;
    }
    if (nodes.length === 0) {
      setError("graph is empty");
      return;
    }
    setSavingVersion(true);
    setError("");
    try {
      const response = await fetch(`/v1/programs/${selectedProgramId}/versions`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ flavor: activeFlavor, ...graphPayload(nodes, edges) })
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const saved = (await response.json()) as ProgramVersionResponse;
      await loadPrograms();
      await loadProgramDetail(selectedProgramId, saved.detail.version);
      setGraphStatus(`saved version v${saved.detail.version}`);
    } catch (err) {
      setError(err instanceof Error ? err.message : "failed to save version");
    } finally {
      setSavingVersion(false);
    }
  }, [activeFlavor, edges, loadProgramDetail, loadPrograms, nodes, selectedProgramId]);

  useEffect(() => {
    setNodes((curr) => applyConnectedInputs(curr, edges));
  }, [edges]);

  useEffect(() => {
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
  }, [edges, nodes]);

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
    setNodes((curr) => applyNodeChanges(changes, curr));
  }, []);

  const onEdgesChange = useCallback((changes: EdgeChange<FlowEdge>[]) => {
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

  return (
    <div className="flex min-h-screen bg-background text-foreground">
      <aside className="w-[250px] border-r bg-card/80 p-4">
        <div className="mb-4 flex items-center gap-2">
          <WandSparkles className="h-5 w-5 text-primary" />
          <div className="text-sm font-semibold">pd-controller</div>
        </div>
        <div className="space-y-1">
          <NavButton
            active={section === "edges"}
            icon={<Server className="h-4 w-4" />}
            label="Edges"
            onClick={() => {
              setSection("edges");
              setEdgeView("list");
            }}
          />
          <NavButton
            active={section === "programs"}
            icon={<FileCode2 className="h-4 w-4" />}
            label="Programs"
            onClick={() => {
              setSection("programs");
              setProgramView("list");
            }}
          />
        </div>
      </aside>

      <main className="min-w-0 flex-1 bg-gradient-to-br from-slate-50 via-white to-sky-50 p-4 lg:p-6">
        {error ? (
          <Card className="mb-4 border-red-300 bg-red-50">
            <CardContent className="p-3 text-sm text-red-700">{error}</CardContent>
          </Card>
        ) : null}

        {section === "edges" ? edgeView === "list" ? (
          <div className="space-y-4">
            <Card className="border-slate-200/80 bg-white/80 backdrop-blur">
              <CardHeader className="pb-3">
                <CardTitle>Edges</CardTitle>
                <CardDescription>Connected workers, health, and rollout status.</CardDescription>
              </CardHeader>
              <CardContent className="grid grid-cols-2 gap-3 sm:grid-cols-4">
                <div className="rounded-md border bg-background/70 p-3">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Total</div>
                  <div className="text-xl font-semibold">{edgeStats.total}</div>
                </div>
                <div className="rounded-md border bg-background/70 p-3">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Healthy</div>
                  <div className="text-xl font-semibold text-emerald-600">{edgeStats.healthy}</div>
                </div>
                <div className="rounded-md border bg-background/70 p-3">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Degraded</div>
                  <div className="text-xl font-semibold text-amber-600">{edgeStats.degraded}</div>
                </div>
                <div className="rounded-md border bg-background/70 p-3">
                  <div className="text-xs uppercase tracking-wide text-muted-foreground">Pending Cmds</div>
                  <div className="text-xl font-semibold">{formatNumber(edgeStats.pending)}</div>
                </div>
              </CardContent>
            </Card>

            <Card className="border-slate-200/80 bg-white/80 backdrop-blur">
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <CardTitle>Fleet List</CardTitle>
                    <CardDescription>Click a row to open edge detail.</CardDescription>
                  </div>
                  <div className="relative w-full max-w-[320px]">
                    <Input
                      value={edgeSearch}
                      onChange={(event) => setEdgeSearch(event.target.value)}
                      placeholder="Search edge name..."
                      className="h-9 pl-8"
                    />
                    <Activity className="pointer-events-none absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="overflow-hidden rounded-lg border">
                  <div className="grid grid-cols-[minmax(160px,1fr)_130px_180px_230px_120px] gap-2 border-b bg-muted/40 px-3 py-2 text-[11px] uppercase tracking-wide text-muted-foreground">
                    <div>Edge</div>
                    <div>Sync</div>
                    <div>Last Seen</div>
                    <div>Applied Program</div>
                    <div>Health</div>
                  </div>
                  <div className="max-h-[66vh] overflow-auto">
                    {filteredEdges.map((edge) => {
                      const health = edgeHealth(edge);
                      return (
                        <button
                          key={edge.edge_id}
                          type="button"
                          onClick={() => selectEdge(edge.edge_id)}
                          className="grid w-full grid-cols-[minmax(160px,1fr)_130px_180px_230px_120px] items-center gap-2 border-b px-3 py-2 text-left text-sm transition hover:bg-muted/50"
                        >
                          <div className="flex items-center gap-2 font-medium">
                            <ChevronRight className="h-4 w-4 text-muted-foreground" />
                            <span className="truncate">{edge.edge_name}</span>
                          </div>
                          <div className={`text-xs font-semibold uppercase ${syncStatusClasses(edge.sync_status)}`}>
                            {edge.sync_status.split("_").join(" ")}
                          </div>
                          <div className="text-sm">{formatUnixMs(edge.last_seen_unix_ms)}</div>
                          <div className="min-w-0 text-sm">
                            {edge.applied_program ? (
                              <div className="flex items-center gap-1.5">
                                <span className="truncate">{edge.applied_program.name}</span>
                                <Badge className="rounded-full px-2 py-0 text-[10px] font-semibold uppercase tracking-wide">
                                  v{edge.applied_program.version}
                                </Badge>
                              </div>
                            ) : (
                              "none"
                            )}
                          </div>
                          <div className={`flex items-center gap-1.5 text-xs uppercase ${edgeHealthClasses(edge)}`}>
                            <Circle className="h-3.5 w-3.5 fill-current" />
                            <span>{health}</span>
                          </div>
                        </button>
                      );
                    })}
                    {filteredEdges.length === 0 ? (
                      <div className="px-3 py-6 text-center text-sm text-muted-foreground">No edges match your search.</div>
                    ) : null}
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        ) : (
          <div className="space-y-4">
            <Card className="border-slate-200/80 bg-white/80 backdrop-blur">
              <CardHeader>
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <CardTitle>Edge Detail</CardTitle>
                    <CardDescription>
                      {selectedEdge ? selectedEdge.summary.edge_name : "No edge selected"}
                    </CardDescription>
                  </div>
                  <Button variant="outline" onClick={() => setEdgeView("list")} className="inline-flex items-center gap-1">
                    <ArrowLeft className="h-4 w-4" />
                    Back To Edges
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                {selectedEdge ? (
                  <div className="space-y-4">
                    <div className="grid grid-cols-2 gap-2">
                      <div className="rounded-md border bg-background/70 p-2">
                        <div className="text-[11px] uppercase tracking-wide text-muted-foreground">Pending</div>
                        <div className="text-lg font-semibold">{selectedEdge.summary.pending_commands}</div>
                      </div>
                      <div className="rounded-md border bg-background/70 p-2">
                        <div className="text-[11px] uppercase tracking-wide text-muted-foreground">Recent Results</div>
                        <div className="text-lg font-semibold">{selectedEdge.summary.recent_results}</div>
                      </div>
                      <div className="rounded-md border bg-background/70 p-2">
                        <div className="text-[11px] uppercase tracking-wide text-muted-foreground">Last Poll</div>
                        <div className="text-sm">{formatUnixMs(selectedEdge.summary.last_poll_unix_ms)}</div>
                      </div>
                      <div className="rounded-md border bg-background/70 p-2">
                        <div className="text-[11px] uppercase tracking-wide text-muted-foreground">Last Result</div>
                        <div className="text-sm">{formatUnixMs(selectedEdge.summary.last_result_unix_ms)}</div>
                      </div>
                    </div>
                    <div className="rounded-md border bg-background/70 p-2">
                      <div className="text-[11px] uppercase tracking-wide text-muted-foreground">Edge UUID</div>
                      <div className="break-all font-mono text-xs">{selectedEdge.summary.edge_id}</div>
                    </div>
                    <div className="rounded-md border bg-background/70 p-2">
                      <div className="text-[11px] uppercase tracking-wide text-muted-foreground">Currently Applied Program</div>
                      <div className="text-sm font-semibold">
                        {selectedEdge.summary.applied_program ? (
                          <div className="flex items-center gap-1.5">
                            <span>{selectedEdge.summary.applied_program.name}</span>
                            <Badge className="rounded-full px-2 py-0 text-[10px] font-semibold uppercase tracking-wide">
                              v{selectedEdge.summary.applied_program.version}
                            </Badge>
                          </div>
                        ) : (
                          "none"
                        )}
                      </div>
                    </div>
                    <div className="space-y-2">
                      <div className="text-sm font-semibold">Traffic Over Time</div>
                      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
                        <div>
                          <div className="mb-1 text-xs uppercase tracking-wide text-muted-foreground">Requests / Poll Interval</div>
                          <LineChart
                            points={selectedEdge.traffic_series}
                            valueFor={(point) => point.requests}
                            stroke="#0284c7"
                            emptyLabel="No request samples yet."
                          />
                        </div>
                        <div className="space-y-2">
                          <div className="mb-1 text-xs uppercase tracking-wide text-muted-foreground">Status Codes / Poll Interval</div>
                          <MultiLineChart
                            points={selectedEdge.traffic_series}
                            series={[
                              { key: "2xx", stroke: "#16a34a", valueFor: (point) => point.status_2xx },
                              { key: "3xx", stroke: "#0ea5e9", valueFor: (point) => point.status_3xx },
                              { key: "4xx", stroke: "#f59e0b", valueFor: (point) => point.status_4xx },
                              { key: "5xx", stroke: "#dc2626", valueFor: (point) => point.status_5xx }
                            ]}
                            hideZeroSeries
                            emptyLabel="No status samples yet."
                          />
                        </div>
                      </div>
                    </div>

                    {selectedEdge.summary.last_telemetry ? (
                      <div className="rounded-md border bg-background/70 p-3 text-xs">
                        <div className="mb-2 text-[11px] uppercase tracking-wide text-muted-foreground">Telemetry Snapshot</div>
                        <div className="grid grid-cols-1 gap-1 sm:grid-cols-2">
                          <div>uptime_seconds: {selectedEdge.summary.last_telemetry.uptime_seconds}</div>
                          <div>program_loaded: {String(selectedEdge.summary.last_telemetry.program_loaded)}</div>
                          <div>debug_session_active: {String(selectedEdge.summary.last_telemetry.debug_session_active)}</div>
                          <div>data_requests_total: {selectedEdge.summary.last_telemetry.data_requests_total}</div>
                          <div>vm_execution_errors_total: {selectedEdge.summary.last_telemetry.vm_execution_errors_total}</div>
                          <div>program_apply_success_total: {selectedEdge.summary.last_telemetry.program_apply_success_total}</div>
                        </div>
                      </div>
                    ) : (
                      <div className="rounded-md border bg-background/70 p-3 text-sm text-muted-foreground">
                        No telemetry has been reported for this edge yet.
                      </div>
                    )}

                    <div className="rounded-md border bg-background/70 p-3">
                      <div className="mb-2 text-sm font-semibold">Apply Program</div>
                      <div className="grid grid-cols-1 gap-2">
                        <div className="space-y-1">
                          <Label htmlFor="apply-program">Program</Label>
                          <select
                            id="apply-program"
                            value={applyProgramId}
                            onChange={(event) => {
                              setApplyProgramId(event.target.value);
                              setApplyVersion("latest");
                            }}
                            className="h-9 w-full rounded-md border bg-background px-2 text-sm"
                          >
                            <option value="">Select program</option>
                            {programs.map((program) => (
                              <option key={program.program_id} value={program.program_id} disabled={program.latest_version === 0}>
                                {program.name} {program.latest_version === 0 ? "(v0 draft)" : ""}
                              </option>
                            ))}
                          </select>
                        </div>
                        <div className="space-y-1">
                          <Label htmlFor="apply-version">Version</Label>
                          <select
                            id="apply-version"
                            value={applyVersion}
                            onChange={(event) => setApplyVersion(event.target.value)}
                            className="h-9 w-full rounded-md border bg-background px-2 text-sm"
                          >
                            <option value="latest">latest</option>
                            {selectedApplyProgram && selectedApplyProgram.latest_version > 0
                              ? Array.from({ length: selectedApplyProgram.latest_version }, (_, index) => index + 1)
                                  .reverse()
                                  .map((version) => (
                                    <option key={version} value={String(version)}>
                                      v{version}
                                    </option>
                                  ))
                              : null}
                          </select>
                        </div>
                        <Button
                          onClick={applyProgramToEdge}
                          disabled={applyLoading || !applyProgramId || (selectedApplyProgram?.latest_version ?? 0) === 0}
                        >
                          {applyLoading ? "Applying" : "Apply To Edge"}
                        </Button>
                        {selectedApplyProgram && selectedApplyProgram.latest_version === 0 ? (
                          <div className="text-xs text-amber-600">This program is still draft v0. Save a version first.</div>
                        ) : null}
                        {applyStatus ? <div className="text-xs text-muted-foreground">{applyStatus}</div> : null}
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="rounded-md border bg-background/70 p-4 text-sm text-muted-foreground">
                    Select a edge from the list first.
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        ) : programView === "list" ? (
          <div className="space-y-4">
            <div className="rounded-2xl border border-slate-200/80 bg-white/80 px-4 py-4 backdrop-blur lg:px-6">
              <div className="flex flex-wrap items-center justify-between gap-4">
                <div>
                  <div className="text-xs uppercase tracking-[0.24em] text-slate-500">Workflow Registry</div>
                  <div className="mt-1 text-2xl font-semibold tracking-tight">Programs</div>
                  <div className="mt-1 text-sm text-muted-foreground">Store, version, and open workflows for editing.</div>
                </div>
                <div className="grid w-full gap-2 sm:max-w-[520px] sm:grid-cols-[1fr_auto]">
                  <Input
                    id="new-program-name"
                    value={newProgramName}
                    onChange={(event) => setNewProgramName(event.target.value)}
                    placeholder="new-program-name"
                    className="h-10"
                  />
                  <Button onClick={createProgram} disabled={creatingProgram}>
                    <Plus className="mr-1 h-4 w-4" />
                    {creatingProgram ? "Creating" : "Create Program"}
                  </Button>
                </div>
              </div>
            </div>

            <section className="rounded-2xl border border-slate-200/80 bg-white/80 p-4 backdrop-blur">
              <div className="mb-3 flex items-center justify-between gap-3">
                <div className="text-sm font-medium text-slate-700">Program Table</div>
                <Input
                  value={programSearch}
                  onChange={(event) => setProgramSearch(event.target.value)}
                  placeholder="Search by name..."
                  className="h-9 w-full max-w-[320px]"
                />
              </div>

              <div className="overflow-hidden rounded-lg border">
                <div className="grid grid-cols-[minmax(220px,1.4fr)_120px_110px_170px] gap-2 border-b bg-muted/40 px-3 py-2 text-[11px] uppercase tracking-wide text-muted-foreground">
                  <div>Program</div>
                  <div>Latest</div>
                  <div>Versions</div>
                  <div>Updated</div>
                </div>
                <div className="max-h-[66vh] overflow-auto">
                  {filteredPrograms.map((program) => (
                    <button
                      key={program.program_id}
                      type="button"
                      onClick={() => selectProgram(program.program_id)}
                      className="grid w-full grid-cols-[minmax(220px,1.4fr)_120px_110px_170px] items-center gap-2 border-b px-3 py-2 text-left text-sm transition hover:bg-muted/50"
                    >
                      <div className="truncate font-medium">{program.name}</div>
                      <div>v{program.latest_version}</div>
                      <div>{program.versions}</div>
                      <div className="text-xs text-muted-foreground">{formatUnixMs(program.updated_unix_ms)}</div>
                    </button>
                  ))}
                  {filteredPrograms.length === 0 ? (
                    <div className="px-3 py-6 text-center text-sm text-muted-foreground">No programs match your search.</div>
                  ) : null}
                </div>
              </div>
            </section>
          </div>
        ) : (
          <div className="space-y-4">
            <Card>
              <CardHeader>
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <CardTitle>Program Detail</CardTitle>
                    <CardDescription>
                      {selectedProgram ? `Edit ${selectedProgram.name}` : "Program composer"}
                    </CardDescription>
                    {selectedProgram ? (
                      <div className="font-mono text-xs text-muted-foreground">UUID: {selectedProgram.program_id}</div>
                    ) : null}
                  </div>
                  <Button
                    variant="outline"
                    onClick={() => setProgramView("list")}
                    className="inline-flex items-center gap-1"
                  >
                    <ArrowLeft className="h-4 w-4" />
                    Back To Programs
                  </Button>
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                {selectedProgram ? (
                  <>
                    <div className="grid grid-cols-1 gap-2 md:grid-cols-[1fr_180px_auto]">
                      <div className="space-y-1">
                        <Label htmlFor="program-name">Program Name</Label>
                        <Input
                          id="program-name"
                          value={programNameDraft}
                          onChange={(event) => setProgramNameDraft(event.target.value)}
                        />
                      </div>
                      <div className="space-y-1">
                        <Label htmlFor="version-select">Version</Label>
                        <select
                          id="version-select"
                          value={selectedVersion !== null ? String(selectedVersion) : ""}
                          onChange={(event) => selectProgramVersion(event.target.value)}
                          className="h-9 w-full rounded-md border bg-background px-2 text-sm"
                        >
                          {selectedProgram.versions.length === 0 ? <option value="0">v0 (draft)</option> : null}
                          {selectedProgram.versions
                            .slice()
                            .sort((a, b) => b.version - a.version)
                            .map((version) => (
                              <option key={version.version} value={String(version.version)}>
                                v{version.version} ({version.flavor})
                              </option>
                            ))}
                        </select>
                      </div>
                      <div className="flex items-end gap-2">
                        <Button variant="secondary" onClick={renameProgram} disabled={renamingProgram}>
                          {renamingProgram ? "Renaming" : "Rename"}
                        </Button>
                        <Button onClick={saveProgramVersion} disabled={savingVersion || nodes.length === 0}>
                          <Save className="mr-1 h-4 w-4" />
                          {savingVersion ? "Saving" : "Save Version"}
                        </Button>
                      </div>
                    </div>
                    {graphStatus ? <div className="text-xs text-muted-foreground">{graphStatus}</div> : null}
                  </>
                ) : (
                  <div className="text-sm text-muted-foreground">Select a program from the Programs table first.</div>
                )}
              </CardContent>
            </Card>

            <div className="space-y-4">
              <div className="relative overflow-hidden rounded-2xl border border-slate-800 bg-slate-950 text-slate-100 shadow-xl">
                <div className="h-[calc(100vh-290px)] min-h-[760px] w-full" onDragOver={(event) => event.preventDefault()} onDrop={onCanvasDrop}>
                  <ReactFlow<FlowNode, FlowEdge>
                    nodes={nodes}
                    edges={edges}
                    nodeTypes={nodeTypes}
                    onNodesChange={onNodesChange}
                    onEdgesChange={onEdgesChange}
                    onConnect={onConnect}
                    onInit={setRfInstance}
                    fitView
                    fitViewOptions={{ padding: 0.22 }}
                    defaultEdgeOptions={{
                      type: "smoothstep",
                      animated: true,
                      style: { stroke: "#22d3ee", strokeWidth: 2 }
                    }}
                  >
                    <Background color="#1e293b" gap={22} size={1} />
                    <MiniMap
                      position="bottom-left"
                      className="!bg-slate-900"
                      nodeColor="#334155"
                      maskColor="rgba(15, 23, 42, 0.45)"
                    />
                    <Controls position="bottom-right" className="!bg-slate-900 !text-slate-200" />
                  </ReactFlow>
                </div>

                <div className="pointer-events-none absolute left-4 top-4 z-20 hidden xl:block">
                  <Card
                    className={`pointer-events-auto overflow-hidden border-slate-700 bg-white/95 text-slate-900 backdrop-blur ${
                      paletteMinimized ? "w-[210px]" : "w-[320px] max-h-[calc(100vh-330px)]"
                    }`}
                  >
                    <CardHeader className={paletteMinimized ? "py-2" : "pb-3"}>
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <CardTitle>Palette</CardTitle>
                          {!paletteMinimized ? <CardDescription>Drag blocks onto the canvas</CardDescription> : null}
                        </div>
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-7 w-7 px-0"
                          onClick={() => setPaletteMinimized((value) => !value)}
                          aria-label={paletteMinimized ? "Expand palette" : "Minimize palette"}
                        >
                          {paletteMinimized ? <Maximize2 className="h-3.5 w-3.5" /> : <Minimize2 className="h-3.5 w-3.5" />}
                        </Button>
                      </div>
                    </CardHeader>
                    {!paletteMinimized ? (
                      <CardContent className="max-h-[calc(100vh-420px)] space-y-3 overflow-auto">
                        <div className="space-y-1">
                          <Label htmlFor="block-search">Search blocks</Label>
                          <Input
                            id="block-search"
                            value={search}
                            onChange={(event) => setSearch(event.target.value)}
                            placeholder="if, header, rate, set..."
                          />
                        </div>
                        {filteredDefinitions.map((definition) => (
                          <div
                            key={definition.id}
                            className="cursor-grab rounded-md border bg-muted/40 p-3 active:cursor-grabbing"
                            draggable
                            onDragStart={(event) => onPaletteDragStart(event, definition.id)}
                          >
                            <div className="mb-1 flex items-center justify-between gap-2">
                              <div className="text-sm font-semibold">{definition.title}</div>
                              <Badge>{definition.category}</Badge>
                            </div>
                            <p className="mb-2 text-xs text-muted-foreground">{definition.description}</p>
                            <Button size="sm" variant="secondary" className="w-full" onClick={() => addNode(definition.id)}>
                              <Plus className="mr-1 h-3.5 w-3.5" />
                              Add to canvas
                            </Button>
                          </div>
                        ))}
                      </CardContent>
                    ) : null}
                  </Card>
                </div>

                <div className="pointer-events-none absolute right-4 top-4 z-20 hidden xl:block">
                  <Card
                    className={`pointer-events-auto overflow-hidden border-slate-700 bg-white/95 backdrop-blur ${
                      codePanelMinimized ? "w-[220px]" : "w-[440px] max-h-[calc(100vh-330px)]"
                    }`}
                  >
                    <CardHeader className={codePanelMinimized ? "py-2" : "pb-2"}>
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <CardTitle>Generated Code</CardTitle>
                          {!codePanelMinimized ? (
                            <CardDescription>render={rendering ? "updating" : "synced"} deploy_flavor={activeFlavor}</CardDescription>
                          ) : null}
                        </div>
                        <Button
                          size="sm"
                          variant="ghost"
                          className="h-7 w-7 px-0"
                          onClick={() => setCodePanelMinimized((value) => !value)}
                          aria-label={codePanelMinimized ? "Expand generated code panel" : "Minimize generated code panel"}
                        >
                          {codePanelMinimized ? <Maximize2 className="h-3.5 w-3.5" /> : <Minimize2 className="h-3.5 w-3.5" />}
                        </Button>
                      </div>
                    </CardHeader>
                    {!codePanelMinimized ? (
                      <CardContent className="max-h-[calc(100vh-410px)] overflow-auto">
                        <Tabs value={activeFlavor} onValueChange={(value) => setActiveFlavor(value as SourceFlavor)}>
                          <TabsList className="grid w-full grid-cols-4">
                            <TabsTrigger value="rustscript">RustScript</TabsTrigger>
                            <TabsTrigger value="javascript">JavaScript</TabsTrigger>
                            <TabsTrigger value="lua">Lua</TabsTrigger>
                            <TabsTrigger value="scheme">Scheme</TabsTrigger>
                          </TabsList>
                          <TabsContent value="rustscript">
                            <HighlightedCode flavor="rustscript" source={source} />
                          </TabsContent>
                          <TabsContent value="javascript">
                            <HighlightedCode flavor="javascript" source={source} />
                          </TabsContent>
                          <TabsContent value="lua">
                            <HighlightedCode flavor="lua" source={source} />
                          </TabsContent>
                          <TabsContent value="scheme">
                            <HighlightedCode flavor="scheme" source={source} />
                          </TabsContent>
                        </Tabs>
                      </CardContent>
                    ) : null}
                  </Card>
                </div>

                <div className="absolute bottom-0 left-0 right-0 border-t border-slate-800 bg-slate-900/70 px-3 py-2 text-xs text-slate-300">
                  nodes={nodes.length} edges={edges.length} selected_nodes={selectedNodeCount} selected_edges={selectedEdgeCount}
                </div>
              </div>

              <div className="grid grid-cols-1 gap-4 xl:hidden">
                <Card className="h-fit">
                  <CardHeader>
                    <CardTitle>Palette</CardTitle>
                    <CardDescription>Drag blocks onto the canvas</CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="space-y-1">
                      <Label htmlFor="block-search-mobile">Search blocks</Label>
                      <Input
                        id="block-search-mobile"
                        value={search}
                        onChange={(event) => setSearch(event.target.value)}
                        placeholder="if, header, rate, set..."
                      />
                    </div>
                    {filteredDefinitions.map((definition) => (
                      <div
                        key={`mobile-${definition.id}`}
                        className="cursor-grab rounded-md border bg-muted/40 p-3 active:cursor-grabbing"
                        draggable
                        onDragStart={(event) => onPaletteDragStart(event, definition.id)}
                      >
                        <div className="mb-1 flex items-center justify-between gap-2">
                          <div className="text-sm font-semibold">{definition.title}</div>
                          <Badge>{definition.category}</Badge>
                        </div>
                        <p className="mb-2 text-xs text-muted-foreground">{definition.description}</p>
                        <Button size="sm" variant="secondary" className="w-full" onClick={() => addNode(definition.id)}>
                          <Plus className="mr-1 h-3.5 w-3.5" />
                          Add to canvas
                        </Button>
                      </div>
                    ))}
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle>Generated Code</CardTitle>
                    <CardDescription>render={rendering ? "updating" : "synced"} deploy_flavor={activeFlavor}</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <Tabs value={activeFlavor} onValueChange={(value) => setActiveFlavor(value as SourceFlavor)}>
                      <TabsList className="grid w-full grid-cols-4">
                        <TabsTrigger value="rustscript">RustScript</TabsTrigger>
                        <TabsTrigger value="javascript">JavaScript</TabsTrigger>
                        <TabsTrigger value="lua">Lua</TabsTrigger>
                        <TabsTrigger value="scheme">Scheme</TabsTrigger>
                      </TabsList>
                      <TabsContent value="rustscript">
                        <HighlightedCode flavor="rustscript" source={source} />
                      </TabsContent>
                      <TabsContent value="javascript">
                        <HighlightedCode flavor="javascript" source={source} />
                      </TabsContent>
                      <TabsContent value="lua">
                        <HighlightedCode flavor="lua" source={source} />
                      </TabsContent>
                      <TabsContent value="scheme">
                        <HighlightedCode flavor="scheme" source={source} />
                      </TabsContent>
                    </Tabs>
                  </CardContent>
                </Card>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
