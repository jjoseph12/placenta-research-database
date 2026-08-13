(() => {
    const pageSize = 25;
    const base = document.body.dataset.siteBase || "/";
    const suffix = document.body.dataset.pageSuffix || "";
    const filterNames = [
        "organism",
        "data_type",
        "library_strategy",
        "trimester",
        "country",
    ];

    const escapeHtml = (value) => String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");

    const display = (value) => {
        const text = String(value ?? "").trim();
        return ["", "No", "N/A", "NA", "None", "nan", "Unknown"].includes(text)
            ? "Not reported"
            : text;
    };

    const entryUrl = (record, anchor = "") =>
        `${base}entry/${record.object_id}${suffix}${anchor}`;

    const selectedFilters = () => Object.fromEntries(
        filterNames.map((name) => [
            name,
            [...document.querySelectorAll(`input[name="${name}"]:checked`)]
                .map((input) => input.value),
        ]),
    );

    const currentQuery = () =>
        document.querySelector(".repository-search input[name='q']").value.trim();

    const buildParams = (page = 1) => {
        const params = new URLSearchParams();
        const query = currentQuery();
        if (query) params.set("q", query);
        if (page > 1) params.set("page", String(page));
        for (const [name, values] of Object.entries(selectedFilters())) {
            for (const value of values) params.append(name, value);
        }
        return params;
    };

    const repositoryUrl = (page = 1) => {
        const params = buildParams(page);
        const queryString = params.toString();
        return `${base}repository${suffix}${queryString ? `?${queryString}` : ""}`;
    };

    const applyUrlState = () => {
        const params = new URLSearchParams(window.location.search);
        const query = params.get("q") || "";
        document.querySelector(".repository-search input[name='q']").value = query;
        document.querySelector("#repositoryFilters input[name='q']").value = query;

        for (const name of filterNames) {
            const selected = new Set(params.getAll(name));
            for (const input of document.querySelectorAll(`input[name="${name}"]`)) {
                input.checked = selected.has(input.value);
            }
            if (selected.size) {
                const section = document.querySelector(`input[name="${name}"]`)?.closest("details.filter-section");
                if (section) section.open = true;
            }
        }
    };

    const matchesFilters = (record, filters) => {
        for (const [name, selected] of Object.entries(filters)) {
            if (!selected.length) continue;
            const values = record.filters[name] || [];
            if (!selected.some((value) => values.includes(value))) return false;
        }
        return true;
    };

    const studyMarkup = (record) => {
        const meta = [escapeHtml(display(record.organism))];
        if (record.sample_size) meta.push(`${escapeHtml(record.sample_size)} samples`);
        if (record.pregnancy_trimester) meta.push(escapeHtml(record.pregnancy_trimester));

        return `
            <article class="study-result">
                <div class="study-result-main">
                    <a class="gse-link" href="${entryUrl(record)}">${escapeHtml(record.gse_id)}</a>
                    <a class="study-title" href="${entryUrl(record)}">${escapeHtml(display(record.title))}</a>
                    <p class="study-result-meta">${meta.join(" &middot; ")}</p>
                </div>
                <dl class="study-result-fields">
                    <div><dt>Assay</dt><dd>${escapeHtml(display(record.data_type))}</dd></div>
                    <div><dt>Strategy</dt><dd>${escapeHtml(display(record.library_strategy))}</dd></div>
                    <div><dt>Sample country</dt><dd>${escapeHtml(display(record.sample_country))}</dd></div>
                </dl>
            </article>`;
    };

    const updateControls = (filters, query) => {
        const selected = Object.values(filters).flat();
        const count = selected.length;
        const heading = document.querySelector(".filter-heading em");
        heading.textContent = count
            ? `${count} selected`
            : "Organism, trimester, assay, country";

        document.querySelector("#clearFilters").hidden = !count && !query;
        document.querySelector("#startOver").hidden = !count && !query;

        const chips = document.querySelector("#activeChips");
        chips.innerHTML = selected
            .map((value) => `<span>${escapeHtml(value)}</span>`)
            .join("");
        chips.hidden = !selected.length;
    };

    const updatePagination = (page, totalPages) => {
        const pagination = document.querySelector("#repositoryPagination");
        const previous = document.querySelector("#previousPage");
        const next = document.querySelector("#nextPage");
        pagination.hidden = totalPages <= 1;
        document.querySelector("#pageStatus").textContent = `Page ${page} of ${totalPages}`;
        previous.hidden = page <= 1;
        previous.href = repositoryUrl(page - 1);
        next.hidden = page >= totalPages;
        next.href = repositoryUrl(page + 1);
    };

    const csvValue = (value) => `"${String(value ?? "").replaceAll('"', '""')}"`;

    const downloadCsv = (records) => {
        const fields = [
            "gse_id",
            "title",
            "organism",
            "sample_size",
            "data_type",
            "library_strategy",
            "pregnancy_trimester",
            "sample_country",
            "evidence_count",
            "repair_count",
        ];
        const rows = [
            fields.join(","),
            ...records.map((record) => fields.map((field) => csvValue(record[field])).join(",")),
        ];
        const blob = new Blob([rows.join("\n")], {type: "text/csv;charset=utf-8"});
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = "placenta_study_index.csv";
        link.click();
        URL.revokeObjectURL(url);
    };

    const initialize = async () => {
        applyUrlState();
        const response = await fetch(`${base}data/studies.json`);
        if (!response.ok) throw new Error("Study index could not be loaded.");
        const studies = await response.json();
        const filters = selectedFilters();
        const query = currentQuery();
        const queryText = query.toLocaleLowerCase();
        const matching = studies.filter((record) =>
            (!queryText || record.search.includes(queryText))
            && matchesFilters(record, filters));

        const requestedPage = Math.max(
            1,
            Number.parseInt(new URLSearchParams(window.location.search).get("page") || "1", 10),
        );
        const totalPages = Math.max(1, Math.ceil(matching.length / pageSize));
        const page = Math.min(requestedPage, totalPages);
        const pageRecords = matching.slice((page - 1) * pageSize, page * pageSize);

        document.querySelector("#resultCount").textContent = matching.length.toLocaleString();
        document.querySelector("#resultContext").textContent = query ? `matching "${query}"` : "";
        document.querySelector(".study-list").innerHTML = pageRecords.map(studyMarkup).join("");
        document.querySelector("#repositoryEmpty").hidden = matching.length > 0;
        updateControls(filters, query);
        updatePagination(page, totalPages);

        document.querySelector(".repository-search").addEventListener("submit", (event) => {
            event.preventDefault();
            window.location.assign(repositoryUrl());
        });
        document.querySelector("#repositoryFilters").addEventListener("submit", (event) => {
            event.preventDefault();
            window.location.assign(repositoryUrl());
        });
        document.querySelector("#downloadCsv").addEventListener("click", (event) => {
            event.preventDefault();
            downloadCsv(matching);
        });
    };

    initialize().catch((error) => {
        document.querySelector(".study-list").innerHTML =
            `<div class="empty-state">${escapeHtml(error.message)}</div>`;
    });
})();
