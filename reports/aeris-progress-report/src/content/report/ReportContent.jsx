import React from "react";

import {
  DataComponent,
  DataTable,
  ReportSection,
  RichNarrative,
  useDataApp,
} from "../../data-app-public.jsx";

const recentProgressColumns = [
  { field: "deliverable", label: "Deliverable", presentation: "identity" },
  { field: "status", label: "Status", presentation: "status" },
  { field: "evidence", label: "Evidence" },
  { field: "remaining", label: "Remaining work" },
];

const roadmapColumns = [
  { field: "phase", label: "Roadmap phase", presentation: "identity" },
  { field: "status", label: "Status", presentation: "status" },
  { field: "evidence", label: "Evidence in repository" },
  { field: "nextGate", label: "Acceptance gate berikutnya" },
];

const capabilityColumns = [
  { field: "area", label: "Capability", presentation: "identity" },
  { field: "status", label: "Status", presentation: "status" },
  { field: "evidence", label: "Evidence" },
];

const datasetColumns = [
  { field: "dataset", label: "Dataset", presentation: "identity" },
  { field: "rows", label: "Rows" },
  { field: "from", label: "From" },
  { field: "to", label: "To" },
  { field: "notes", label: "Notes" },
];

const riskColumns = [
  { field: "priority", label: "Priority", presentation: "status" },
  { field: "risk", label: "Risk", presentation: "identity" },
  { field: "evidence", label: "Evidence" },
  { field: "action", label: "Required action" },
];

export function ReportContent() {
  const {
    snapshot,
    visible,
    canEdit,
    mode,
    appTitle,
    setAppTitle,
  } = useDataApp();

  const recentProgress = snapshot.queries.recent_progress?.rows ?? [];
  const roadmap = snapshot.queries.roadmap_status?.rows ?? [];
  const capabilities = snapshot.queries.capability_status?.rows ?? [];
  const datasets = snapshot.queries.dataset_inventory?.rows ?? [];
  const risks = snapshot.queries.risk_register?.rows ?? [];

  return <article className="report-content" aria-label="AERIS project progress report">
    <header className="report-hero">
      <div className="report-kicker">Project progress review · 4 September 2026</div>
      <h1
        data-data-app-title
        contentEditable={canEdit && mode === "edit"}
        suppressContentEditableWarning
        aria-label={canEdit && mode === "edit" ? "Edit report heading" : undefined}
        onBlur={canEdit && mode === "edit" ? (event) => setAppTitle(event.currentTarget.textContent.trim() || appTitle) : undefined}
        onKeyDown={canEdit && mode === "edit" ? (event) => {
          if (event.key === "Enter") {
            event.preventDefault();
            event.currentTarget.blur();
          }
        } : undefined}
      >{appTitle}</h1>
      <RichNarrative
        id="report:introduction"
        className="report-deck"
        label="Edit report introduction"
        value="AERIS kini memiliki koleksi historis ISPU yang incremental dan weather enrichment yang lebih kaya. Progres data preparation meningkat, tetapi canonical dataset belum lolos quality gate dan nilai utama PRD—monitor, predict, explain, personalize, act—belum tersedia sebagai alur produk end-to-end."
      />
    </header>

    {visible("report-summary") && <ReportSection
      id="report-summary"
      title="Kesimpulan eksekutif"
      queryId="capability_status"
      queryIds={["capability_status", "recent_progress", "dataset_inventory"]}
      sourceRowsByQuery={{
        capability_status: capabilities,
        recent_progress: recentProgress,
        dataset_inventory: datasets,
      }}
      sourceRows={capabilities}
      showHeading={false}
      className="report-summary"
    >
      <RichNarrative
        id="report-summary:body"
        className="report-summary-lead"
        label="Edit executive summary"
        value={`## Kesimpulan eksekutif

Posisi proyek tetap **Phase 1 yang belum selesai, dengan data preparation Phase 2 yang semakin matang**. Sejak snapshot sebelumnya, collector ISPU sudah incremental, seluruh 4.885 station-days telah diperiksa tanpa request error, enriched weather menghasilkan 116.640 hourly rows, dan utility merge ISPU sudah tersedia.

Quality gate belum lolos: **ispu_master.parquet** mengandung lima malformed rows, 44,4% valid hourly ISPU rows kehilangan semua nilai polutan, HC kosong seluruhnya, dan output canonical merge belum dibuat. PostgreSQL/Redis, backend product endpoints, frontend, dan ML juga belum terhubung. Dataset tetap harus diperlakukan sebagai **working data**, bukan ground truth siap training.`}
      />
    </ReportSection>}

    {visible("report-recent-progress") && <section className="report-section">
      <RichNarrative
        id="report-recent-progress:intro"
        className="report-analysis"
        label="Edit recent progress interpretation"
        value="## Progres sejak snapshot sebelumnya\n\nKemajuan utama berada pada ingestion dan enrichment. Pengumpulan data sudah jauh lebih reproducible, tetapi keberadaan file master belum sama dengan dataset yang siap dipakai model."
      />
      <DataComponent
        id="report-recent-progress"
        title="New deliverables and remaining gates"
        queryId="recent_progress"
        kind="table"
        displayRows={recentProgress}
        sourceRows={recentProgress}
        description="Perubahan yang dapat dibuktikan dari kode, status log, dan output lokal."
      >
        <DataTable rows={recentProgress} columns={recentProgressColumns} rowKey="deliverable" caption="Recent AERIS progress" searchable={false} />
      </DataComponent>
    </section>}

    {visible("report-roadmap") && <section className="report-section">
      <RichNarrative
        id="report-roadmap:intro"
        className="report-analysis"
        label="Edit roadmap interpretation"
        value="## Status terhadap roadmap PRD\n\nPhase 1 memiliki fondasi dan historical ingestion yang lebih lengkap, tetapi acceptance gate produknya belum tercapai. Phase 2 berada pada data preparation; Phase 3 dan Phase 5 belum dimulai, sedangkan Phase 4 baru memiliki utility awal."
      />
      <DataComponent
        id="report-roadmap"
        title="Roadmap progress and next gates"
        queryId="roadmap_status"
        kind="table"
        displayRows={roadmap}
        sourceRows={roadmap}
        description="Status kualitatif berdasarkan artefak yang dapat diperiksa, bukan estimasi persentase usaha."
      >
        <DataTable rows={roadmap} columns={roadmapColumns} rowKey="phase" caption="AERIS roadmap progress" searchable={false} />
      </DataComponent>
    </section>}

    {visible("report-capabilities") && <section className="report-section">
      <RichNarrative
        id="report-capabilities:intro"
        className="report-analysis"
        label="Edit capability interpretation"
        value="## Capability inventory\n\nDua capability sudah benar-benar terbentuk sebagai fondasi: struktur monorepo dan orkestrasi Docker Compose. Sebagian besar area lain masih partial karena komponennya ada tetapi belum terhubung end-to-end."
      />
      <DataComponent
        id="report-capabilities"
        title="PRD capability versus repository evidence"
        queryId="capability_status"
        kind="table"
        displayRows={capabilities}
        sourceRows={capabilities}
        description="Gunakan pencarian untuk menemukan capability tertentu."
      >
        <DataTable rows={capabilities} columns={capabilityColumns} rowKey="area" caption="AERIS capability inventory" />
      </DataComponent>
    </section>}

    {visible("report-data-inventory") && <section className="report-section">
      <RichNarrative
        id="report-data-inventory:intro"
        className="report-analysis"
        label="Edit dataset interpretation"
        value={`## Data yang sudah tersedia

Historical ingestion adalah progres paling kuat. ISPU menyediakan hourly grid lengkap untuk lima wilayah sampai 3 September 2026, dan enriched weather sampai 29 Agustus 2026. Namun kelengkapan grid tidak berarti kelengkapan nilai: hanya 65.190 dari 117.240 valid ISPU rows memiliki minimal satu polutan. Semua file data tetap lokal dan diabaikan Git.`}
      />
      <DataComponent
        id="report-data-inventory"
        title="Local dataset inventory"
        queryId="dataset_inventory"
        kind="table"
        displayRows={datasets}
        sourceRows={datasets}
        description="Snapshot file lokal; row count tidak termasuk header."
      >
        <DataTable rows={datasets} columns={datasetColumns} rowKey="dataset" caption="AERIS local dataset inventory" searchable={false} compactNumbers={false} />
      </DataComponent>
    </section>}

    {visible("report-risks") && <section className="report-section">
      <RichNarrative
        id="report-risks:intro"
        className="report-analysis"
        label="Edit risk interpretation"
        value="## Risiko yang menghalangi milestone berikutnya\n\nFokus P0 sekarang adalah memperbaiki ISPU master, menetapkan aturan missingness/semantik polutan, membangun canonical merged dataset, lalu menghubungkannya ke aplikasi."
      />
      <DataComponent
        id="report-risks"
        title="Technical risk register"
        queryId="risk_register"
        kind="table"
        displayRows={risks}
        sourceRows={risks}
        description="P0 menghalangi validitas model atau vertical slice; P1 memengaruhi reliability dan reproducibility."
      >
        <DataTable rows={risks} columns={riskColumns} rowKey="risk" caption="AERIS technical risk register" searchable={false} />
      </DataComponent>
    </section>}

    {visible("report-plan") && <ReportSection
      id="report-plan"
      title="Recommended execution order"
      queryId="risk_register"
      sourceRows={risks}
      showHeading={false}
      className="report-plan"
    >
      <RichNarrative
        id="report-plan:body"
        className="report-caveat"
        label="Edit execution plan"
        value={`## Urutan eksekusi yang direkomendasikan

1. **Perbaiki build ISPU master.** Exclude status log dari glob, drop/assert null join keys, lalu regenerate dan audit ulang.
2. **Finalisasi canonical data contract.** Pertahankan pemisahan source/target identity dan UTC/local time; kunci unit serta semantik ISPU, availability rules, dan station registry.
3. **Bangun canonical merged dataset.** Gabungkan base pollution, ISPU, dan enriched weather dengan lineage, missing indicators, chronological policy, serta quality report.
4. **Rapikan reproducibility.** Pilih satu entry point ISPU, hilangkan absolute paths, lengkapi dependency, tambah tests, dan versioning dataset.
5. **Bangun vertical slice aplikasi.** Simpan observations/stations ke PostgreSQL/PostGIS, buat endpoint historical/latest, lalu dashboard sederhana.
6. **Bangun baseline ML.** Mulai dari persistence/seasonal baseline, lalu XGBoost; evaluasi per horizon dan wilayah sebelum SHAP/anomaly.
7. **Lanjutkan intelligence dan MLOps.** Implementasikan anomaly, attribution, alert, AI brief, Prefect, MLflow, DVC, CI/CD, dan monitoring setelah alur inti stabil.

**Definition of done milestone terdekat:** satu canonical dataset berhasil dibangun ulang tanpa malformed/duplicate join keys, memiliki quality report per sumber/wilayah/polutan, dan seluruh keputusan unit, timezone, station mapping, serta missingness terdokumentasi dan diuji.`}
      />
    </ReportSection>}

    <RichNarrative
      id="report:limitations"
      className="report-disclosure"
      label="Edit limitations"
      value="Evidence cutoff: 4 September 2026. Review ini memakai PRD, repository/status Git, status log ISPU, CSV lokal, dan dua Parquet master. Prospective ISPU join dihitung read-only; output merge belum dibuat. Endpoint eksternal dan runtime Docker/frontend tidak diuji."
    />
  </article>;
}
