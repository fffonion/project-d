import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { graphPayload, normalizeFlavor, toFlowEdges } from "@/app/helpers";
import type {
  ProgramDetailResponse,
  ProgramListResponse,
  ProgramSummary,
  ProgramVersionResponse
} from "@/app/types";
import { initialSource } from "@/app/types";
import type { ComposerProgramApi } from "@/app/hooks/useComposer";

type UseProgramsArgs = {
  onError: (message: string) => void;
  showProgramsSection: () => void;
  onProgramDeleted: (programId: string) => void;
  composer: ComposerProgramApi;
};

export function usePrograms({ onError, showProgramsSection, onProgramDeleted, composer }: UseProgramsArgs) {
  const {
    activeFlavor,
    edges,
    isCodeEditMode,
    nodes,
    source,
    setActiveFlavor,
    setSource,
    setIsCodeEditMode,
    setGraphStatus,
    toFlowNodes,
    hydrateGraph,
    clearHydrationState,
    clearGraphForCodeVersion,
    resetComposerToDraft
  } = composer;

  const makeDefaultProgramName = useCallback(() => `program-${Date.now()}`, []);

  const [programs, setPrograms] = useState<ProgramSummary[]>([]);
  const [selectedProgramId, setSelectedProgramId] = useState<string | null>(null);
  const [selectedProgram, setSelectedProgram] = useState<ProgramDetailResponse | null>(null);
  const [selectedVersion, setSelectedVersion] = useState<number | null>(null);
  const [programView, setProgramView] = useState<"list" | "composer">("list");
  const [programSearch, setProgramSearch] = useState("");
  const [programNameDraft, setProgramNameDraft] = useState("");
  const [creatingProgram, setCreatingProgram] = useState(false);
  const [savingVersion, setSavingVersion] = useState(false);
  const [renamingProgram, setRenamingProgram] = useState(false);
  const [deletingProgram, setDeletingProgram] = useState(false);

  const programLoadSeqRef = useRef(0);
  const programLoadAbortRef = useRef<AbortController | null>(null);

  const loadPrograms = useCallback(async () => {
    const response = await fetch("/v1/programs");
    if (!response.ok) {
      throw new Error(`failed to load programs (${response.status})`);
    }
    const data = (await response.json()) as ProgramListResponse;
    setPrograms(data.programs);
  }, []);

  useEffect(() => {
    return () => {
      programLoadAbortRef.current?.abort();
      clearHydrationState();
    };
  }, [clearHydrationState]);

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
          setSelectedVersion(0);
          setIsCodeEditMode(false);
          clearGraphForCodeVersion();
          setSource(initialSource);
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
          clearGraphForCodeVersion();
          setGraphStatus(`loaded v${version.detail.version} (code edited)`);
          return;
        }

        const { loadedNodes, skippedNodes } = toFlowNodes(version.detail.nodes);
        if (!isCurrent()) {
          clearHydrationState();
          return;
        }
        if (version.detail.nodes.length === 0) {
          clearHydrationState();
          throw new Error("loaded program version has no nodes; keeping previous graph");
        }
        if (version.detail.nodes.length > 0 && loadedNodes.length === 0) {
          clearHydrationState();
          throw new Error(`failed to load program graph: ${skippedNodes} node(s) use unknown block types`);
        }

        const loadedEdges = toFlowEdges(version.detail.edges, loadedNodes);
        if (version.detail.edges.length > 0 && loadedEdges.length === 0) {
          onError("loaded graph has edges that do not match current block handles; showing nodes only");
        }
        hydrateGraph(loadedNodes, loadedEdges, isCurrent);
        setGraphStatus(`loaded v${version.detail.version}`);
      } catch (err) {
        clearHydrationState();
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
    [clearGraphForCodeVersion, clearHydrationState, hydrateGraph, onError, setActiveFlavor, setGraphStatus, setIsCodeEditMode, setSource, toFlowNodes]
  );

  const selectProgram = useCallback(
    async (programId: string) => {
      setSelectedProgramId(programId);
      setProgramView("composer");
      setGraphStatus("");
      onError("");
      try {
        await loadProgramDetail(programId);
      } catch (err) {
        onError(err instanceof Error ? err.message : "failed to load program");
      }
    },
    [loadProgramDetail, onError, setGraphStatus]
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
      onError("");
      try {
        await loadProgramDetail(selectedProgramId, version);
      } catch (err) {
        onError(err instanceof Error ? err.message : "failed to load version");
      }
    },
    [loadProgramDetail, onError, selectedProgramId, setGraphStatus, setIsCodeEditMode]
  );

  const createProgram = useCallback(async () => {
    setCreatingProgram(true);
    onError("");
    try {
      const generatedProgramName = makeDefaultProgramName();
      const response = await fetch("/v1/programs", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name: generatedProgramName })
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const created = (await response.json()) as ProgramDetailResponse;
      await loadPrograms();
      await selectProgram(created.program_id);
      showProgramsSection();
      setGraphStatus("program created");
    } catch (err) {
      onError(err instanceof Error ? err.message : "failed to create program");
    } finally {
      setCreatingProgram(false);
    }
  }, [loadPrograms, makeDefaultProgramName, onError, selectProgram, setGraphStatus, showProgramsSection]);

  const renameProgram = useCallback(async () => {
    if (!selectedProgramId) {
      return;
    }
    if (!programNameDraft.trim()) {
      onError("program name cannot be empty");
      return;
    }
    setRenamingProgram(true);
    onError("");
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
      onError(err instanceof Error ? err.message : "failed to rename program");
    } finally {
      setRenamingProgram(false);
    }
  }, [loadProgramDetail, loadPrograms, onError, programNameDraft, selectedProgramId, selectedVersion, setGraphStatus]);

  const deleteProgram = useCallback(
    async (programId: string, programName: string) => {
      const confirmed = window.confirm(`Delete program "${programName}" and all versions? This cannot be undone.`);
      if (!confirmed) {
        return;
      }
      setDeletingProgram(true);
      onError("");
      try {
        const response = await fetch(`/v1/programs/${programId}`, {
          method: "DELETE"
        });
        if (!response.ok) {
          throw new Error(await response.text());
        }
        await loadPrograms();
        onProgramDeleted(programId);
        if (selectedProgramId === programId) {
          setSelectedProgramId(null);
          setSelectedProgram(null);
          setSelectedVersion(null);
          resetComposerToDraft();
          setProgramView("list");
          setGraphStatus("program deleted");
        }
      } catch (err) {
        onError(err instanceof Error ? err.message : "failed to delete program");
      } finally {
        setDeletingProgram(false);
      }
    },
    [loadPrograms, onError, onProgramDeleted, resetComposerToDraft, selectedProgramId, setGraphStatus]
  );

  const saveProgramVersion = useCallback(async () => {
    if (!selectedProgramId) {
      onError("select a program first");
      return;
    }
    if (!isCodeEditMode && nodes.length === 0) {
      onError("graph is empty");
      return;
    }
    setSavingVersion(true);
    onError("");
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
        hydrateGraph(loadedNodes, loadedEdges);
      } else {
        clearGraphForCodeVersion();
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
      onError(err instanceof Error ? err.message : "failed to save version");
    } finally {
      setSavingVersion(false);
    }
  }, [activeFlavor, clearGraphForCodeVersion, edges, hydrateGraph, isCodeEditMode, loadProgramDetail, loadPrograms, nodes, onError, selectedProgramId, setActiveFlavor, setGraphStatus, setIsCodeEditMode, setSource, source, toFlowNodes]);

  const filteredPrograms = useMemo(() => {
    const keyword = programSearch.trim().toLowerCase();
    if (!keyword) {
      return programs;
    }
    return programs.filter((program) => program.name.toLowerCase().includes(keyword));
  }, [programSearch, programs]);

  return {
    programs,
    selectedProgramId,
    selectedProgram,
    selectedVersion,
    programView,
    setProgramView,
    programSearch,
    setProgramSearch,
    programNameDraft,
    setProgramNameDraft,
    creatingProgram,
    savingVersion,
    renamingProgram,
    deletingProgram,
    filteredPrograms,
    loadPrograms,
    selectProgram,
    selectProgramVersion,
    createProgram,
    renameProgram,
    deleteProgram,
    saveProgramVersion
  };
}
