(function () {
  "use strict";

  let POLL_MS = 1500; // overridden by the queue_poll_seconds setting once loadSettings() runs

  // -- helpers --------------------------------------------------------
  async function api(path, options) {
    const resp = await fetch(path, {
      headers: { "Content-Type": "application/json" },
      ...options,
    });
    if (resp.status === 401) {
      window.location.href = "/login";
      throw new Error("Signed out -- redirecting to login.");
    }
    let data = null;
    try {
      data = await resp.json();
    } catch (e) {
      data = null;
    }
    if (!resp.ok) {
      const message = (data && data.error) || `Request failed (${resp.status})`;
      throw new Error(message);
    }
    return data;
  }

  // Every .hint status line in the app goes through here -- prefixing an
  // icon in one place means all of them (search status, settings save
  // status, Movies/Series row-action errors, bulk-action summaries, ...)
  // pick it up for free instead of needing an icon added at each call site.
  function setStatus(el, message, isError) {
    el.innerHTML = "";
    el.style.color = isError ? "var(--danger)" : "var(--text-muted)";
    if (!message) return;
    const icon = document.createElement("span");
    icon.className = "material-symbols-outlined icon-inline";
    icon.textContent = isError ? "error" : "info";
    el.appendChild(icon);
    el.appendChild(document.createTextNode(message));
  }

  // Runs `worker` over every item in `items`, but never more than `limit`
  // requests in flight at once -- same shape/return as Promise.allSettled
  // (an array of {status, value|reason} in input order), just throttled.
  // A bulk action can span thousands of rows (a big library); firing one
  // fetch per row with a plain Promise.allSettled(items.map(worker))
  // dumps all of them on the wire simultaneously, which is exactly what
  // was blowing out "select all" bulk actions with "Failed to fetch" --
  // browsers cap concurrent connections per origin well below that, and
  // Flask's dev server doesn't fare any better against a burst that size.
  async function mapWithConcurrency(items, worker, limit = 6) {
    const results = new Array(items.length);
    let next = 0;
    async function runNext() {
      while (next < items.length) {
        const i = next++;
        try {
          results[i] = { status: "fulfilled", value: await worker(items[i]) };
        } catch (err) {
          results[i] = { status: "rejected", reason: err };
        }
      }
    }
    await Promise.all(Array.from({ length: Math.min(limit, items.length) }, runNext));
    return results;
  }

  function fmtStatus(status) {
    if (!status) return "";
    if (status === "ready") return "Ready";
    if (status === "running") return "Exporting";
    if (status === "downloading") return "Downloading";
    if (status === "download_queued") return "Queued";
    if (status === "download_error") return "Download error";
    return status.charAt(0).toUpperCase() + status.slice(1);
  }

  // "1.2 GB" / "850 MB" -- used by download progress text, which tracks
  // actual bytes transferred rather than just a file count (see
  // buildRowDescriptor's task_type === "download" branch).
  function formatBytes(bytes) {
    if (bytes == null || Number.isNaN(bytes)) return "";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let value = bytes;
    let unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
      value /= 1024;
      unit++;
    }
    const decimals = unit === 0 ? 0 : value < 10 ? 2 : 1;
    return `${value.toFixed(decimals)} ${units[unit]}`;
  }

  // Sums a download task's per-item byte progress (see tasks.py's
  // bytes_downloaded/bytes_total item fields) into one aggregate --
  // shared by the Downloads/Activity row (buildRowDescriptor) and the
  // media detail page's live panel (renderDownloadTaskPanel) so both
  // read it the same way. bytesTotalKnown is false whenever any
  // not-yet-finished item hasn't reported a Content-Length yet (a queued
  // item that hasn't started, or a server that never sent one) -- in
  // that case bytesTotal is a floor, not the real total, so callers
  // should only trust the ratio/ETA once it's true.
  function downloadByteTotals(d) {
    const total = d.total_items || 0;
    let bytesDone = 0, bytesTotal = 0, bytesTotalKnown = total > 0;
    for (const it of d.items || []) {
      bytesDone += it.bytes_downloaded || 0;
      if (it.bytes_total != null) bytesTotal += it.bytes_total;
      else if (it.status !== "done") bytesTotalKnown = false;
    }
    return { bytesDone, bytesTotal, bytesTotalKnown };
  }

  // -- download speed / ETA -------------------------------------------------
  // The server only ever reports a point-in-time bytes_downloaded (see
  // tasks.py) -- no throughput history -- so speed/ETA are derived
  // client-side, from consecutive polls. Updated exactly once per poll
  // tick, from fresh /api/tasks data only (see refreshTasks()'s call to
  // updateDownloadRate) -- never from a re-render triggered by paging,
  // filtering, or sorting, which would otherwise scramble the delta.
  // Smoothed (exponential moving average) so one unusually slow/fast
  // poll interval doesn't make the ETA jump around.
  const DOWNLOAD_RATE_SMOOTHING = 0.3;
  const downloadRateState = new Map(); // task id -> {time, bytes, rate (bytes/sec)}

  function updateDownloadRate(taskId, bytesDone) {
    const now = Date.now();
    const prev = downloadRateState.get(taskId);
    let rate = prev ? prev.rate : 0;
    if (prev && now > prev.time) {
      const deltaBytes = bytesDone - prev.bytes;
      const deltaSeconds = (now - prev.time) / 1000;
      if (deltaBytes >= 0) {
        const instantRate = deltaBytes / deltaSeconds;
        rate = prev.rate ? (DOWNLOAD_RATE_SMOOTHING * instantRate + (1 - DOWNLOAD_RATE_SMOOTHING) * prev.rate) : instantRate;
      }
    }
    downloadRateState.set(taskId, { time: now, bytes: bytesDone, rate });
    return rate;
  }

  function formatEta(remainingBytes, rate) {
    if (!rate || rate <= 0 || remainingBytes <= 0) return "";
    const seconds = remainingBytes / rate;
    if (!Number.isFinite(seconds)) return "";
    if (seconds < 5) return "almost done";
    if (seconds < 60) return `~${Math.round(seconds)}s left`;
    const totalMinutes = Math.floor(seconds / 60);
    if (totalMinutes < 60) return `~${totalMinutes}m left`;
    const hours = Math.floor(totalMinutes / 60);
    const minutes = totalMinutes % 60;
    if (hours < 24) return `~${hours}h ${minutes}m left`;
    const days = Math.floor(hours / 24);
    return `~${days}d ${hours % 24}h left`;
  }

  // -- page navigation (sidebar) ----------------------------------------
  // Each page carries its own heading in its content now (see
  // .page-heading in the HTML) -- the header has no page title of its
  // own anymore, just the global search bar (see below).
  const KNOWN_PAGES = new Set(["activity", "downloads", "movies", "series", "links", "logs", "settings"]);
  // media-detail/log-detail aren't real nav destinations (no sidebar item
  // points at either, and neither is worth remembering across reloads
  // since each needs a specific id) -- reached via a Movies/Series row or
  // a Logs row instead.
  const navItems = document.querySelectorAll(".nav-item");
  let lastListPage = "activity"; // where media-detail's Back button returns to

  // -- sidebar toggle (logo click) -------------------------------------
  const sidebarEl = document.querySelector(".sidebar");
  const sidebarToggleBtn = document.getElementById("sidebar-toggle");
  const sidebarBackdrop = document.getElementById("sidebar-backdrop");

  function closeMobileSidebar() {
    if (sidebarEl && window.innerWidth <= 768) {
      sidebarEl.classList.remove("open");
    }
    if (sidebarBackdrop) sidebarBackdrop.classList.remove("open");
  }

  function toggleSidebar() {
    if (!sidebarEl) return;
    if (window.innerWidth <= 768) {
      const isOpen = sidebarEl.classList.toggle("open");
      if (sidebarBackdrop) sidebarBackdrop.classList.toggle("open", isOpen);
    } else {
      sidebarEl.classList.toggle("collapsed");
    }
  }

  if (sidebarToggleBtn) {
    sidebarToggleBtn.addEventListener("click", toggleSidebar);
  }
  if (sidebarBackdrop) {
    sidebarBackdrop.addEventListener("click", closeMobileSidebar);
  }

  function showPage(page) {
    if (!KNOWN_PAGES.has(page) && page !== "media-detail" && page !== "log-detail") page = "activity";
    document.querySelectorAll(".page").forEach((el) => {
      el.classList.toggle("active", el.id === `page-${page}`);
    });
    document.querySelectorAll(".toolbar-group").forEach((el) => {
      el.classList.toggle("hidden", el.dataset.toolbar !== page);
    });
    navItems.forEach((el) => el.classList.toggle("active", el.dataset.page === page));
    // The Settings sub-tabs only make sense while Settings itself is the
    // active page -- collapsed back into the single nav item otherwise.
    const settingsSubnavEl = document.getElementById("settings-subnav");
    if (settingsSubnavEl) {
      settingsSubnavEl.classList.toggle("expanded", page === "settings");
      if (page === "settings") showSettingsTab(localStorage.getItem("febarr-settings-tab") || "connections");
    }
    if (KNOWN_PAGES.has(page)) {
      lastListPage = page;
      localStorage.setItem("febarr-page", page);
    }
    if (page === "movies" || page === "series") loadMediaListNow(page);
    if (page === "links") loadLinks();
    closeMobileSidebar();
  }
  navItems.forEach((el) => {
    el.addEventListener("click", () => showPage(el.dataset.page));
  });
  // Not called yet -- showPage() can land on Movies/Series, which reach
  // into media-library state declared further down this file. Called at
  // the very bottom instead, once everything above actually exists.

  // -- settings --------------------------------------------------------
  const loginCookieInput = document.getElementById("login_cookie");
  const loginCookieStatus = document.getElementById("login_cookie_status");
  const tmdbKeyInput = document.getElementById("tmdb_api_key");
  const tmdbKeyStatus = document.getElementById("tmdb_api_key_status");
  const minReleaseYearInput = document.getElementById("min_release_year");
  const seriesYearInFolderInput = document.getElementById("series_year_in_folder");
  const autoQueueDiscoveredInput = document.getElementById("auto_queue_discovered");
  const flaresolverrInput = document.getElementById("flaresolverr_url");
  const exportRootInput = document.getElementById("export_root");
  const maxWorkersInput = document.getElementById("max_parallel_workers");
  const maxDownloadsInput = document.getElementById("max_parallel_downloads");
  const resyncStaleDaysInput = document.getElementById("resync_stale_days");
  const requestTimeoutInput = document.getElementById("request_timeout_seconds");
  const maxRetriesInput = document.getElementById("max_retries");
  const maxFolderDepthInput = document.getElementById("max_folder_depth");
  const userAgentInput = document.getElementById("user_agent");
  const queuePollSecondsInput = document.getElementById("queue_poll_seconds");

  async function loadSettings() {
    const s = await api("/api/settings");
    if (loginCookieInput) loginCookieInput.value = s.login_cookie || "";
    if (tmdbKeyInput) tmdbKeyInput.value = s.tmdb_api_key || "";
    if (minReleaseYearInput) minReleaseYearInput.value = s.min_release_year || 0;
    if (seriesYearInFolderInput) seriesYearInFolderInput.checked = s.series_year_in_folder !== false;
    if (autoQueueDiscoveredInput) autoQueueDiscoveredInput.checked = !!s.auto_queue_discovered;
    if (flaresolverrInput) flaresolverrInput.value = s.flaresolverr_url || "";
    if (exportRootInput) exportRootInput.value = s.export_root || "";
    if (maxWorkersInput) maxWorkersInput.value = s.max_parallel_workers || 2;
    if (maxDownloadsInput) maxDownloadsInput.value = s.max_parallel_downloads || 1;
    if (resyncStaleDaysInput) resyncStaleDaysInput.value = s.resync_stale_days || 0;
    if (requestTimeoutInput) requestTimeoutInput.value = s.request_timeout_seconds || 20;
    if (maxRetriesInput) maxRetriesInput.value = s.max_retries != null ? s.max_retries : 5;
    if (maxFolderDepthInput) maxFolderDepthInput.value = s.max_folder_depth || 8;
    if (userAgentInput) userAgentInput.value = s.user_agent || "";
    if (queuePollSecondsInput) queuePollSecondsInput.value = s.queue_poll_seconds || 1.5;
    if (loginCookieStatus) {
      loginCookieStatus.textContent = s.login_cookie_set ? "Saved" : "";
    }
    if (tmdbKeyStatus) {
      tmdbKeyStatus.textContent = s.tmdb_api_key_set ? "Saved" : "";
    }
    POLL_MS = Math.max(500, (Number(s.queue_poll_seconds) || 1.5) * 1000);
    applyAccountStatus(s);
    return s;
  }

  const headerSaveSettingsBtn = document.getElementById("header-save-settings-btn");
  const headerSettingsSaveStatus = document.getElementById("header-settings-save-status");

  if (headerSaveSettingsBtn) {
    headerSaveSettingsBtn.addEventListener("click", () => {
      const activeSubpage = document.querySelector(".settings-subpage.active");
      if (!activeSubpage) return;
      const form = activeSubpage.querySelector("form");
      if (form) {
        if (typeof form.requestSubmit === "function") {
          form.requestSubmit();
        } else {
          form.dispatchEvent(new Event("submit", { cancelable: true, bubbles: true }));
        }
      }
    });
  }

  // Each settings sub-tab (Connections/Library/Advanced) is its own form,
  // saved independently -- but they all PATCH the same /api/settings
  // endpoint and refresh from the same loadSettings(), so one saved field
  // never stomps another tab's unsaved edits. Feedback goes to the one
  // header-level status line (headerSettingsSaveStatus) -- these forms
  // don't carry their own per-form status element anymore.
  function wireSettingsForm(formId, buildPatch) {
    const form = document.getElementById(formId);
    if (!form) return;
    form.addEventListener("submit", async (e) => {
      e.preventDefault();
      setStatus(headerSettingsSaveStatus, "Saving...", false);
      try {
        await api("/api/settings", { method: "POST", body: JSON.stringify(buildPatch()) });
        setStatus(headerSettingsSaveStatus, "Saved.", false);
        await loadSettings();
      } catch (err) {
        setStatus(headerSettingsSaveStatus, err.message, true);
      }
    });
  }

  wireSettingsForm("settings-form-connections", () => ({
    login_cookie: loginCookieInput.value.trim(),
    tmdb_api_key: tmdbKeyInput.value.trim(),
    flaresolverr_url: flaresolverrInput.value.trim(),
  }));
  wireSettingsForm("settings-form-library", () => ({
    min_release_year: Number(minReleaseYearInput.value) || 0,
    series_year_in_folder: seriesYearInFolderInput.checked,
    auto_queue_discovered: autoQueueDiscoveredInput.checked,
    export_root: exportRootInput.value.trim(),
    max_parallel_workers: Number(maxWorkersInput.value),
    max_parallel_downloads: Number(maxDownloadsInput.value),
    resync_stale_days: Number(resyncStaleDaysInput.value),
  }));
  wireSettingsForm("settings-form-advanced", () => ({
    request_timeout_seconds: Number(requestTimeoutInput.value) || 20,
    max_retries: Number(maxRetriesInput.value),
    max_folder_depth: Number(maxFolderDepthInput.value) || 8,
    user_agent: userAgentInput.value.trim(),
    queue_poll_seconds: Number(queuePollSecondsInput.value) || 1.5,
  }));

  // -- settings sub-tabs (Connections/Library/Advanced/Account) ---------
  // Nested one level under the Settings nav item: showSettingsTab() swaps
  // which .settings-subpage is visible and which sidebar sub-item is
  // marked active, same pattern as the top-level showPage().
  const settingsSubnav = document.getElementById("settings-subnav");
  const settingsSubitems = settingsSubnav ? settingsSubnav.querySelectorAll(".nav-subitem") : [];
  const KNOWN_SETTINGS_TABS = new Set(["connections", "library", "advanced", "account"]);

  function showSettingsTab(tab) {
    if (!KNOWN_SETTINGS_TABS.has(tab)) tab = "connections";
    document.querySelectorAll(".settings-subpage").forEach((el) => {
      el.classList.toggle("active", el.id === `settings-tab-${tab}`);
    });
    settingsSubitems.forEach((el) => el.classList.toggle("active", el.dataset.settingsTab === tab));
    localStorage.setItem("febarr-settings-tab", tab);
    if (headerSettingsSaveStatus) headerSettingsSaveStatus.textContent = "";
  }
  settingsSubitems.forEach((el) => {
    el.addEventListener("click", (e) => {
      e.stopPropagation(); // don't let the click bubble up to the Settings nav-item itself
      showSettingsTab(el.dataset.settingsTab);
      closeMobileSidebar();
    });
  });

  // -- account / login --------------------------------------------------
  const accountForm = document.getElementById("account-form");
  const accountUsernameInput = document.getElementById("account_username");
  const accountPasswordInput = document.getElementById("account_password");
  const accountConfirmInput = document.getElementById("account_confirm_password");
  const accountConfiguredStatus = document.getElementById("account-configured-status");
  const accountSaveStatus = document.getElementById("account-save-status");
  const disableAccountBtn = document.getElementById("disable-account-btn");

  function applyAccountStatus(s) {
    accountConfiguredStatus.textContent = s.auth_configured
      ? `Enabled (${s.auth_username})`
      : "Disabled";
    disableAccountBtn.classList.toggle("hidden", !s.auth_configured);
    if (document.activeElement !== accountUsernameInput) {
      accountUsernameInput.value = s.auth_username || "";
    }
  }

  accountForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    setStatus(accountSaveStatus, "Saving...", false);
    setStatus(headerSettingsSaveStatus, "Saving...", false);
    try {
      const s = await api("/api/account", {
        method: "POST",
        body: JSON.stringify({
          username: accountUsernameInput.value.trim(),
          password: accountPasswordInput.value,
          confirm_password: accountConfirmInput.value,
        }),
      });
      accountPasswordInput.value = "";
      accountConfirmInput.value = "";
      setStatus(accountSaveStatus, "Saved.", false);
      setStatus(headerSettingsSaveStatus, "Saved.", false);
      applyAccountStatus(s);
    } catch (err) {
      setStatus(accountSaveStatus, err.message, true);
      setStatus(headerSettingsSaveStatus, err.message, true);
    }
  });

  disableAccountBtn.addEventListener("click", async () => {
    try {
      const s = await api("/api/account", { method: "DELETE" });
      setStatus(accountSaveStatus, "Login disabled.", false);
      setStatus(headerSettingsSaveStatus, "Login disabled.", false);
      applyAccountStatus(s);
    } catch (err) {
      setStatus(accountSaveStatus, err.message, true);
      setStatus(headerSettingsSaveStatus, err.message, true);
    }
  });

  // -- global search: paste a Febbox link to add a share, or search your
  // existing library by title -- one bar covers both, no separate "look
  // it up on TMDB" mode (that was a standalone lookup nothing else in
  // the app used -- auto-classification/posters still use TMDB
  // themselves, just not as something you search through here). A
  // pasted share link streams straight to the library: every group with
  // a confident TMDB match is added the moment it's found (watch it
  // happen live in Movies/Series), no review/confirm step; anything
  // without a match is denied instead -- shown in Activity, never added.
  // A plain title instead searches titles already in Movies/Series
  // (discovered, queued, or ready) -- click a result to jump straight to
  // it, same as the (now-removed) per-page search boxes used to filter,
  // just from one place that works regardless of which page you're on.
  // --------------------------------------------------------------------
  const CATEGORY_LABELS = { tv: "TV", movie: "Movies", anime: "Anime", anime_movie: "Anime Movies" };
  const FEBBOX_LINK_RE = /febbox\.com\/share\//i;

  const globalSearchForm = document.getElementById("global-search-form");
  const globalSearchInput = document.getElementById("global-search-input");
  const searchResultsPanel = document.getElementById("search-results-panel");
  const searchStatus = document.getElementById("search-status");
  const searchResultsList = document.getElementById("search-results-list");

  let pendingAnalyzeJobId = null; // job we started, waiting for the poll loop to track its progress

  // Searches the actual library (both types, a fresh fetch each time --
  // simpler and more correct than trusting mediaListCache, which is only
  // populated once a Movies/Series page has actually been visited) and
  // renders matching titles as clickable rows; each jumps straight to
  // that title (its detail page if it's on disk, otherwise just the
  // Movies/Series list it belongs to, since there's nothing to view yet).
  async function searchLibrary(query) {
    const q = query.toLowerCase();
    const [movies, series] = await Promise.all([
      api("/api/media?type=movie"),
      api("/api/media?type=series"),
    ]);
    const tagged = [
      ...movies.map((m) => ({ ...m, page: "movies" })),
      ...series.map((s) => ({ ...s, page: "series" })),
    ];
    return tagged.filter((item) => item.title.toLowerCase().includes(q));
  }

  function renderSearchResults(results) {
    searchResultsList.innerHTML = "";
    if (results.length === 0) {
      // No results - no popup displayed
      return;
    }
    for (const item of results) {
      const row = document.createElement("div");
      row.className = "search-result-row";
      row.tabIndex = 0;
      const info = document.createElement("div");
      info.className = "search-result-info";
      const title = document.createElement("div");
      title.className = "search-result-title";
      title.textContent = item.year ? `${item.title} (${item.year})` : item.title;
      const meta = document.createElement("div");
      meta.className = "search-result-meta";
      meta.textContent = [MEDIA_STATUS_LABELS[item.status] || fmtStatus(item.status), CATEGORY_LABELS[item.category] || item.category]
        .filter(Boolean).join(" · ");
      info.appendChild(title);
      info.appendChild(meta);
      row.appendChild(info);
      const open = () => {
        searchResultsPanel.classList.add("hidden");
        showMediaDetail(item.id, item.page);
        globalSearchInput.value = "";
      };
      row.addEventListener("click", open);
      row.addEventListener("keydown", (e) => { if (e.key === "Enter") open(); });
      searchResultsList.appendChild(row);
    }
  }

  globalSearchForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    const query = globalSearchInput.value.trim();
    if (!query) return;

    if (FEBBOX_LINK_RE.test(query)) {
      // A real share link -- kick off the streaming analyze-and-catalog
      // pipeline silently in the background, not a library search.
      searchResultsPanel.classList.add("hidden");
      searchResultsList.classList.add("hidden");
      searchResultsList.innerHTML = "";
      setStatus(searchStatus, "", false);
      globalSearchInput.value = "";
      try {
        const job = await api("/api/shares/analyze", {
          method: "POST",
          body: JSON.stringify({ share_link: query }),
        });
        pendingAnalyzeJobId = job.id;
        await refreshTasks();
      } catch (err) {
        console.error(err);
      }
    } else {
      // Library search
      setStatus(searchStatus, "", false);
      try {
        const results = await searchLibrary(query);
        if (results.length > 0) {
          searchResultsPanel.classList.remove("hidden");
          searchResultsList.classList.remove("hidden");
          renderSearchResults(results);
        } else {
          searchResultsPanel.classList.add("hidden");
          searchResultsList.classList.add("hidden");
        }
      } catch (err) {
        searchResultsPanel.classList.add("hidden");
        console.error(err);
      }
    }
  });

  document.addEventListener("click", (e) => {
    if (!e.target.closest("#search-results-panel") && !e.target.closest("#global-search-form")) {
      searchResultsPanel.classList.add("hidden");
    }
  });

  // -- queue-table machinery ------------------------------------------------
  // Activity is "a list of {kind, data} items rendered as a paginated/
  // filterable table" -- createTableView() below owns the filter/page
  // state and DOM rendering for it.
  const queueRowTemplate = document.getElementById("queue-row-template");

  function timeAgo(iso) {
    if (!iso) return "";
    const then = new Date(iso).getTime();
    if (Number.isNaN(then)) return "";
    const seconds = Math.max(0, Math.floor((Date.now() - then) / 1000));
    if (seconds < 60) return "just now";
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    return `${Math.floor(hours / 24)}d ago`;
  }

  // -- queue pause/resume --------------------------------------------------
  // One shared pause switch on the server (tasks_paused -- see
  // tasks.TaskManager.set_paused()) covers both the export and download
  // queues; Activity and Downloads each get their own button reflecting
  // and driving that same switch, wired identically.
  const queuePauseBtn = document.getElementById("queue-pause-btn");
  const queuePauseIcon = document.getElementById("queue-pause-icon");
  const queuePauseLabel = document.getElementById("queue-pause-label");
  const queueStatusText = document.getElementById("queue-status-text");
  const downloadsPauseBtn = document.getElementById("downloads-pause-btn");
  const downloadsPauseIcon = document.getElementById("downloads-pause-icon");
  const downloadsPauseLabel = document.getElementById("downloads-pause-label");
  const downloadsStatusText = document.getElementById("downloads-status-text");

  function wirePauseButton(btn, statusEl) {
    if (!btn) return;
    btn.addEventListener("click", async () => {
      btn.disabled = true;
      try {
        const paused = btn.dataset.paused === "true";
        await api(paused ? "/api/tasks/resume_queue" : "/api/tasks/pause", { method: "POST" });
        await refreshTasks();
      } catch (err) {
        setStatus(statusEl, err.message, true);
      } finally {
        btn.disabled = false;
      }
    });
  }
  wirePauseButton(queuePauseBtn, queueStatusText);
  wirePauseButton(downloadsPauseBtn, downloadsStatusText);

  // -- Activity bulk actions -------------------------------------------
  // Selection itself lives inside activityView (createTableView's
  // selectable option) so it survives paging/filtering/polling; this
  // just reflects that state into the bulk bar and dispatches each
  // button to the matching per-task endpoint, run over every selected
  // task that's actually eligible (ineligible ones are silently
  // skipped, same as a per-row action button that wouldn't be shown for
  // that status -- the summary afterwards says how many actually ran).
  // Cancel is the only bulk action left -- Activity/Downloads only ever
  // show a task while it's queued/running now (see refreshTasks()), so
  // Resume (needs "error") and Resync (needs "done") never have
  // anything eligible to act on here anymore; both are still reachable
  // per-row from Logs, which keeps the full history including those.
  const activityBulkBar = document.getElementById("activity-bulk-bar");
  const activityBulkCount = document.getElementById("activity-bulk-count");
  const activitySelectAllCheckbox = document.getElementById("queue-select-all-checkbox");
  const queueSelectBtn = document.getElementById("queue-select-btn");
  const activityPageEl = document.getElementById("page-activity");
  let activitySelectionMode = false;

  function updateActivityBulkBar(selectedItems) {
    const n = selectedItems.length;
    const isMode = activitySelectionMode || n > 0;
    if (activityPageEl) activityPageEl.classList.toggle("selection-active", isMode);
    if (queueSelectBtn) queueSelectBtn.classList.toggle("active", isMode);
    activityBulkBar.classList.toggle("hidden", !isMode);
    activityBulkCount.textContent = `${n} selected`;
    const filteredCount = activityView.filteredCount();
    activitySelectAllCheckbox.checked = n > 0 && n === filteredCount;
    activitySelectAllCheckbox.indeterminate = n > 0 && n < filteredCount;
    setSelectAllToggle(bulkSelectAllBtn, n > 0 && n === filteredCount);
  }

  if (queueSelectBtn) {
    queueSelectBtn.addEventListener("click", () => {
      activitySelectionMode = !activitySelectionMode;
      if (!activitySelectionMode) {
        activityView.clearSelection();
      }
      updateActivityBulkBar(activityView.getSelectedItems());
    });
  }

  activitySelectAllCheckbox.addEventListener("change", () => {
    if (activitySelectAllCheckbox.checked) {
      activitySelectionMode = true;
      activityView.selectAllFiltered();
    } else {
      activityView.clearSelection();
    }
  });

  // Shared by both Activity's and Downloads' bulk bars -- run `call` over
  // every selected row `predicate` accepts, same silent-skip-if-
  // ineligible behavior a per-row action button already has, summarized
  // in `statusEl` afterwards.
  async function runBulkAction(view, statusEl, predicate, call, label) {
    const targets = view.getSelectedItems().filter((it) => predicate(it.data)).map((it) => it.data.id);
    if (targets.length === 0) {
      setStatus(statusEl, `No selected item is eligible for ${label}.`, true);
      return;
    }
    const results = await mapWithConcurrency(targets, call);
    const failed = results.filter((r) => r.status === "rejected").length;
    view.clearSelection();
    await refreshTasks();
    setStatus(
      statusEl,
      failed ? `${label}: ${targets.length - failed}/${targets.length} succeeded, ${failed} failed.`
        : `${label}: ${targets.length} item(s) done.`,
      !!failed
    );
  }
  const runTaskBulkAction = (predicate, call, label) => runBulkAction(activityView, queueStatusText, predicate, call, label);

  const bulkSelectAllBtn = document.getElementById("bulk-select-all-btn");
  if (bulkSelectAllBtn) {
    // Doubles as "Deselect all" once everything filtered is already
    // selected -- see setSelectAllToggle(); this is also what dropping
    // a selection entirely now goes through, since there's no separate
    // Close button anymore.
    bulkSelectAllBtn.addEventListener("click", () => {
      const n = activityView.getSelectedItems().length;
      if (n > 0 && n === activityView.filteredCount()) activityView.clearSelection();
      else activityView.selectAllFiltered();
    });
  }
  document.getElementById("bulk-cancel-btn").addEventListener("click", () =>
    runTaskBulkAction((d) => d.status === "queued" || d.status === "running",
      (id) => api(`/api/tasks/${id}/cancel`, { method: "POST" }), "Cancel"));

  // -- Downloads bulk actions --------------------------------------------
  // Same shape as Activity's, above, just pointed at downloadsView/its
  // own bulk bar.
  const downloadsBulkBar = document.getElementById("downloads-bulk-bar");
  const downloadsBulkCount = document.getElementById("downloads-bulk-count");
  const downloadsSelectAllCheckbox = document.getElementById("downloads-select-all-checkbox");
  const downloadsSelectBtn = document.getElementById("downloads-select-btn");
  const downloadsPageEl = document.getElementById("page-downloads");
  let downloadsSelectionMode = false;

  function updateDownloadsBulkBar(selectedItems) {
    const n = selectedItems.length;
    const isMode = downloadsSelectionMode || n > 0;
    if (downloadsPageEl) downloadsPageEl.classList.toggle("selection-active", isMode);
    if (downloadsSelectBtn) downloadsSelectBtn.classList.toggle("active", isMode);
    downloadsBulkBar.classList.toggle("hidden", !isMode);
    downloadsBulkCount.textContent = `${n} selected`;
    const filteredCount = downloadsView.filteredCount();
    downloadsSelectAllCheckbox.checked = n > 0 && n === filteredCount;
    downloadsSelectAllCheckbox.indeterminate = n > 0 && n < filteredCount;
    setSelectAllToggle(downloadsBulkSelectAllBtn, n > 0 && n === filteredCount);
  }

  if (downloadsSelectBtn) {
    downloadsSelectBtn.addEventListener("click", () => {
      downloadsSelectionMode = !downloadsSelectionMode;
      if (!downloadsSelectionMode) {
        downloadsView.clearSelection();
      }
      updateDownloadsBulkBar(downloadsView.getSelectedItems());
    });
  }

  downloadsSelectAllCheckbox.addEventListener("change", () => {
    if (downloadsSelectAllCheckbox.checked) {
      downloadsSelectionMode = true;
      downloadsView.selectAllFiltered();
    } else {
      downloadsView.clearSelection();
    }
  });

  const runDownloadsBulkAction = (predicate, call, label) => runBulkAction(downloadsView, downloadsStatusText, predicate, call, label);

  const downloadsBulkSelectAllBtn = document.getElementById("downloads-bulk-select-all-btn");
  downloadsBulkSelectAllBtn.addEventListener("click", () => {
    const n = downloadsView.getSelectedItems().length;
    if (n > 0 && n === downloadsView.filteredCount()) downloadsView.clearSelection();
    else downloadsView.selectAllFiltered();
  });
  document.getElementById("downloads-bulk-cancel-btn").addEventListener("click", () =>
    runDownloadsBulkAction((d) => d.status === "queued" || d.status === "running",
      (id) => api(`/api/tasks/${id}/cancel`, { method: "POST" }), "Cancel"));

  // Movie vs series bucket, from whichever item carries a category.
  // Items with no derivable type (e.g. an analyze job covering a mix of
  // titles) always pass, since there's no single type to filter them on.
  function itemMediaBucket(item) {
    const d = item.data;
    if (item.kind === "task" || item.kind === "denied") {
      if (!d.category) return null;
      return d.category === "tv" || d.category === "anime" ? "series" : "movie";
    }
    return null;
  }
  function itemMatchesType(item, typeFilter) {
    if (typeFilter === "all") return true;
    const bucket = itemMediaBucket(item);
    return bucket === null || bucket === typeFilter;
  }

  // "active" (default) = still in motion or waiting to be -- everything
  // except finished/errored work (or a cancelled analyze job -- tasks
  // themselves never settle on "cancelled", see tasks.TaskManager.
  // on_settled, but an analyze job legitimately can). Owned/completed
  // items never show unless you ask for them via Completed or All.
  function itemMatchesFilter(item, filter) {
    const d = item.data;
    if (filter === "all") return true;
    if (filter === "rejected") return item.kind === "denied";
    if (item.kind === "denied") return false; // only ever shows via Rejected or All
    if (filter === "running") return item.kind === "task" && d.status === "running";
    if (filter === "queued") return item.kind === "task" && d.status === "queued";
    if (filter === "background") return item.kind === "analyze";
    if (filter === "completed") {
      return d.status === "done" || d.status === "error" || d.status === "cancelled";
    }
    if (item.kind === "task") return d.status === "running" || d.status === "queued";
    return d.status === "running"; // analyze: only the still-running ones count as active
  }

  function toMs(iso) {
    const t = iso ? Date.parse(iso) : NaN;
    return Number.isNaN(t) ? 0 : t;
  }

  // [priority, tiebreaker] -- lower priority sorts first. Running always
  // ranks above queued, so with the queue unpaused the top
  // max_parallel_workers rows are always what's actually running; queued
  // tasks keep their true FIFO order (ascending created_at) so the row
  // right below is genuinely next in line, not just recently added.
  function queueSortKey(item) {
    const d = item.data;
    if (item.kind === "task") {
      if (d.status === "running") return [0, toMs(d.started_at || d.created_at)];
      if (d.status === "queued") return [2, toMs(d.created_at)];
      return [4, -toMs(d.finished_at || d.started_at || d.created_at)];
    }
    if (item.kind === "denied") return [5, d.index || 0];
    if (d.status === "running") return [1, -toMs(d.started_at)];
    return [4, -toMs(d.finished_at || d.started_at)];
  }

  // mode "activity" (default) builds the normal manage-this-item actions
  // (Run now/Cancel/Resume/Resync -- deliberately no Remove, see below).
  // mode "logs" swaps those out for a single "View log" action -- see
  // showLogDetail() -- since the Logs page is a read-only view over the
  // same task/analyze-job data.
  function buildRowDescriptor(item, mode) {
    const d = item.data;

    if (item.kind === "denied") {
      const catLabel = d.category ? (CATEGORY_LABELS[d.category] || d.category) : "";
      return {
        key: `denied:${d.job_id}:${d.index}`, status: "denied", statusLabel: "Rejected",
        title: d.year ? `${d.title} (${d.year})` : d.title,
        meta: [catLabel, `from "${d.raw_name}"`].filter(Boolean).join(" · "),
        typeLabel: catLabel || "Rejected", isBackground: false, selectable: false,
        progressPct: null, progressText: d.reason, actions: [], logLines: [],
      };
    }

    if (item.kind === "task") {
      const isDownload = d.task_type === "download";
      // A download task only ever has one phase ("downloading"), so
      // there's nothing worth appending the way export's "Exporting ·
      // discovering/exporting" does -- just "Downloading" (fmtStatus's
      // "running" -> "Exporting" mapping is export-specific, so this
      // can't reuse it directly for a download's running state).
      const label = isDownload
        ? (d.status === "running" ? "Downloading" : fmtStatus(d.status))
        : (d.status === "running" && d.phase ? `${fmtStatus(d.status)} · ${d.phase}` : fmtStatus(d.status));
      const catLabel = d.category ? (CATEGORY_LABELS[d.category] || d.category) : "";
      const meta = [catLabel, `share: ${d.share_key}`].filter(Boolean).join(" · ");

      const total = d.total_items || 0;
      const done = d.files_written || 0;
      let progressPct = null;
      let progressText;

      if (isDownload) {
        // Byte-level, not just file-count -- a download task is often
        // just one or two very large files, where "0/1 files" would sit
        // unchanged for the whole transfer. See tasks.py's
        // bytes_downloaded/bytes_total per-item fields.
        const { bytesDone, bytesTotal, bytesTotalKnown } = downloadByteTotals(d);
        if (bytesTotalKnown && bytesTotal > 0) {
          progressPct = Math.min(100, Math.round((bytesDone / bytesTotal) * 100));
        } else if (d.status === "running") {
          progressPct = "indeterminate";
        } else if (d.status === "done") {
          progressPct = 100;
        }
        const parts = [];
        if (bytesDone > 0 || bytesTotal > 0) {
          parts.push(bytesTotalKnown && bytesTotal > 0
            ? `${formatBytes(bytesDone)} / ${formatBytes(bytesTotal)}`
            : `${formatBytes(bytesDone)} downloaded so far`);
        }
        parts.push(`${done}/${total} file(s)`);
        if (d.items_failed) parts.push(`${d.items_failed} failed`);
        if (d.status === "running") {
          const rate = (downloadRateState.get(d.id) || {}).rate || 0;
          if (rate > 0) parts.push(`${formatBytes(rate)}/s`);
          if (bytesTotalKnown && bytesTotal > 0) {
            const eta = formatEta(bytesTotal - bytesDone, rate);
            if (eta) parts.push(eta);
          }
        }
        progressText = parts.join(" · ");
      } else {
        if (d.phase === "discovering" && d.status === "running") progressPct = "indeterminate";
        else if (total > 0) progressPct = Math.min(100, Math.round((done / total) * 100));
        else if (d.status === "done") progressPct = 100;

        if (d.phase === "discovering") {
          progressText = `Discovering... ${total} item(s) found so far`;
        } else {
          const parts = [`${done}/${total} stream link(s) recorded`];
          if (d.items_failed) parts.push(`${d.items_failed} failed`);
          progressText = parts.join(" · ");
        }
      }
      if (d.status === "running" && d.current_item) progressText += ` — ${d.current_item}`;
      if (d.status !== "running" && d.error) progressText = d.error;
      if (!isDownload && d.status === "done" && d.last_synced_at) progressText += ` (synced ${timeAgo(d.last_synced_at)})`;

      const active = d.status === "queued" || d.status === "running";
      const actions = [];
      if (d.status === "queued") {
        actions.push({ label: "Run now", cls: "btn-primary", onClick: async () => {
          await api(`/api/tasks/${d.id}/run_now`, { method: "POST" });
          await refreshTasks();
        } });
      }
      if (active) {
        actions.push({ label: "Cancel", cls: "btn-danger", onClick: async () => {
          await api(`/api/tasks/${d.id}/cancel`, { method: "POST" });
          await refreshTasks();
        } });
      }
      if (d.status === "error") {
        actions.push({ label: "Resume", cls: "btn-ghost", onClick: async () => {
          await api(`/api/tasks/${d.id}/resume`, { method: "POST" });
          await refreshTasks();
        } });
      }
      if (d.status === "done" && !isDownload) {
        // Not meaningful for a download task -- there's no .strm link
        // left to refresh once the real file has landed (see tasks.py's
        // resync_task()/_check_auto_resync(), both export-only).
        actions.push({ label: "Resync", cls: "btn-ghost", onClick: async () => {
          await api(`/api/tasks/${d.id}/resync`, { method: "POST" });
          await refreshTasks();
        } });
      }
      // Deliberately no "Remove" here -- Cancel only ever stops a task,
      // it never deletes anything: a queued task settles straight back
      // into a discovered library item, and a running one does the same
      // once its worker actually notices (see tasks.TaskManager.
      // on_settled) -- there's no "cancelled" task left sitting around
      // to remove. Hard removal -- gone from the queue *and* the
      // library -- is a Movies/Series-only action (mediaRowActions'
      // "Remove"), not Activity's.

      const taskKey = `task:${d.id}`;
      const viewLogAction = { label: "View log", cls: "btn-ghost", onClick: () => showLogDetail(taskKey) };
      // Logs is now where a settled task's own management actions live
      // too (Resume/Resync), not just its log -- Activity/Downloads only
      // ever show a task while it's still queued/running (see
      // refreshTasks()), so a done/errored one would have nothing to
      // click there anymore. Logs keeps the full history, so it's where
      // those actions have to move to stay reachable at all.
      const rowActions = mode === "logs" ? [...actions, viewLogAction] : actions;

      return {
        key: taskKey, status: d.status, statusLabel: label,
        title: d.target_path, meta, typeLabel: isDownload ? "Download" : "Export", isBackground: false, selectable: true,
        progressPct, progressText, actions: rowActions, logLines: d.log || [],
      };
    }

    // analyze job
    let progressText;
    if (d.status === "running") {
      progressText = d.current || "Starting...";
      if (d.discovered.length) progressText += ` (${d.discovered.length} added so far)`;
    } else if (d.status === "done") {
      const parts = [`${d.discovered.length} added to library`];
      if (d.denied.length) parts.push(`${d.denied.length} not added`);
      progressText = parts.join(", ");
    } else if (d.status === "cancelled") progressText = `Cancelled -- ${d.discovered.length} added before stopping.`;
    else progressText = d.error || "Failed";
    const actions = [];
    if (d.status === "running") {
      actions.push({ label: "Cancel", cls: "btn-danger", onClick: async () => {
        await api(`/api/shares/analyze/${d.id}/cancel`, { method: "POST" });
        await refreshTasks();
      } });
    } else {
      actions.push({ label: "Dismiss", cls: "btn-ghost", onClick: async () => {
        try {
          await api(`/api/shares/analyze/${d.id}`, { method: "DELETE" });
        } catch (err) {
          // already gone -- fine
        }
        await refreshTasks();
      } });
    }
    const analyzeKey = `analyze:${d.id}`;
    const logActions = [{ label: "View log", cls: "btn-ghost", onClick: () => showLogDetail(analyzeKey) }];
    return {
      key: analyzeKey, status: d.status, statusLabel: d.status === "running" ? "Analyzing" : fmtStatus(d.status),
      title: `Analyze: ${d.share_key}`, meta: "", typeLabel: "Background", isBackground: true, selectable: false,
      progressPct: d.status === "running" ? "indeterminate" : null,
      progressText, actions: mode === "logs" ? logActions : actions, logLines: d.log || [],
    };
  }

  // Every row/bulk action button is icon-only (see mountActionButtons and
  // the icon spans hand-set on the static bulk-bar buttons in
  // index.html) -- action.label still carries the real word, it's just
  // shown as the button's title/aria-label tooltip instead of on-button
  // text, so a row of five actions doesn't turn into a wall of words.
  // Values are Material Symbols ligature names (fonts.google.com/icons),
  // rendered via a .material-symbols-outlined span, not raw glyphs.
  const ACTION_ICONS = {
    "Run now": "fast_forward", // jump the queue
    "Cancel": "stop",          // halt a queued/running task, never deletes it (Activity and Movies/Series)
    "Resume": "play_arrow",    // retry an errored task
    "Resync": "refresh",       // re-fetch stream links
    "Recheck": "refresh",      // re-run Analyze on a saved link (Links page)
    "Queue": "add",            // promote a discovered item into a real export
    "Dismiss": "close",        // discard a finished/cancelled analyze job record
    "Remove": "delete",        // hard delete: exported files + queue/library record, gone (Movies/Series only)
    "View": "chevron_right",   // open the detail page
    "View log": "article",     // open the log
    "Download": "download",    // turn a streamable (.strm) file/title into a real file on disk
    "Revert to stream": "undo", // undo a download -- back to .strm, from the saved link
  };

  // Every bulk bar's "Select all" button doubles as "Deselect all" once
  // everything currently filtered is already selected -- one button
  // instead of a separate, now-redundant "Close"/"Clear" button (that
  // used to be the only other way to drop a full selection). Assumes
  // the button's markup is `<span class="material-symbols-outlined">
  // icon</span>Label` -- the icon span is looked up and swapped, the
  // trailing text node (the label) is replaced or appended.
  function setSelectAllToggle(btn, allSelected) {
    if (!btn) return;
    const label = allSelected ? "Deselect all" : "Select all";
    const icon = btn.querySelector(".material-symbols-outlined");
    if (icon) icon.textContent = allSelected ? "deselect" : "select_all";
    btn.title = label;
    btn.setAttribute("aria-label", label);
    const lastNode = btn.lastChild;
    if (lastNode && lastNode.nodeType === Node.TEXT_NODE) {
      lastNode.textContent = label;
    } else {
      btn.appendChild(document.createTextNode(label));
    }
  }

  // Mounts a row's action buttons, each running its onClick with the
  // button disabled for the duration and any thrown error surfaced via
  // onError -- shared between the Activity/Logs queue rows and the
  // Movies/Series media rows so both report failures the same way.
  function mountActionButtons(container, actions, onError) {
    container.innerHTML = "";
    for (const action of actions) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = `btn btn-small btn-icon ${action.cls || "btn-ghost"}`;
      const iconName = ACTION_ICONS[action.label];
      if (iconName) {
        const icon = document.createElement("span");
        icon.className = "material-symbols-outlined";
        icon.textContent = iconName;
        btn.appendChild(icon);
      } else {
        btn.textContent = action.label;
      }
      btn.title = action.title || action.label;
      btn.setAttribute("aria-label", action.label);
      if (action.disabled) btn.disabled = true;
      btn.onclick = async () => {
        btn.disabled = true;
        try {
          await action.onClick();
        } catch (err) {
          if (onError) onError(err);
        } finally {
          if (!action.disabled) btn.disabled = false;
        }
      };
      container.appendChild(btn);
    }
  }

  // rowOpts.selectable gates whether the row keeps its checkbox cell at
  // all (Logs rows don't -- see createTableView's `selectable` option);
  // rowOpts.onToggleSelect/isSelected wire the checkbox itself when it's
  // kept, skipped for a descriptor that opted out (denied/analyze rows
  // aren't bulk-actionable, see buildRowDescriptor's `selectable` field).
  function renderQueueRow(descriptor, rowOpts) {
    rowOpts = rowOpts || {};
    const frag = document.importNode(queueRowTemplate.content, true);
    const mainRow = frag.querySelector(".queue-row");
    mainRow.dataset.id = descriptor.key;

    const selectCell = mainRow.querySelector(".q-select-cell");
    if (!rowOpts.selectable) {
      selectCell.remove();
    } else if (!descriptor.selectable) {
      selectCell.innerHTML = "";
    } else {
      const cb = selectCell.querySelector(".q-row-select");
      cb.checked = !!(rowOpts.isSelected && rowOpts.isSelected());
      cb.addEventListener("change", () => rowOpts.onToggleSelect && rowOpts.onToggleSelect(cb.checked));
    }

    const badge = mainRow.querySelector(".q-status-badge");
    badge.textContent = descriptor.statusLabel;
    badge.className = `q-status-badge status-badge status-${descriptor.status}`;

    mainRow.querySelector(".q-title").textContent = descriptor.title;
    mainRow.querySelector(".q-meta").textContent = descriptor.meta;

    const typeTag = mainRow.querySelector(".q-type-tag");
    typeTag.textContent = descriptor.typeLabel;
    typeTag.className = `q-type-tag${descriptor.isBackground ? " bg" : ""}`;

    const progBar = mainRow.querySelector(".q-progress-bar");
    const progFill = progBar.querySelector(".progress-fill");
    progFill.className = "progress-fill";
    if (descriptor.progressPct === "indeterminate") {
      progBar.classList.remove("hidden");
      progFill.classList.add("is-indeterminate");
    } else if (typeof descriptor.progressPct === "number") {
      progBar.classList.remove("hidden");
      progFill.style.width = `${descriptor.progressPct}%`;
      if (descriptor.status === "done") progFill.classList.add("is-done");
      if (descriptor.status === "error") progFill.classList.add("is-error");
    } else {
      progBar.classList.add("hidden");
    }
    mainRow.querySelector(".q-progress-text").textContent = descriptor.progressText || "";

    const actionsCell = mainRow.querySelector(".q-actions");
    const errorStatusEl = rowOpts.errorStatusEl || queueStatusText;
    mountActionButtons(actionsCell, descriptor.actions, (err) => setStatus(errorStatusEl, err.message, true));

    return mainRow;
  }

  // Mounts one independent table view: own filter-bar/type-filter-bar
  // (either can be omitted), own page state, own DOM, own localStorage
  // keys, and (opts.selectable) its own bulk-selection state -- a Set of
  // row keys that survives paging/filtering/polling refreshes (pruned
  // only when an id disappears from the underlying items entirely).
  // Returns { setItems, ... } -- see the bottom of this function for the
  // full surface a caller gets.
  function createTableView(opts) {
    const { tbody, pageInfo, prevBtn, nextBtn, pageSizeSelect, filterBar, typeFilterBar,
      filterKey, typeFilterKey, pageSizeKey, defaultFilter, emptyText, rowMode,
      selectable, onSelectionChange, errorStatusEl } = opts;

    let items = [];
    let page = 1;
    let filter = filterBar ? (localStorage.getItem(filterKey) || defaultFilter) : "all";
    let typeFilter = typeFilterBar ? (localStorage.getItem(typeFilterKey) || "all") : "all";
    let pageSize = parseInt(localStorage.getItem(pageSizeKey) || "20", 10) || 20;
    let lastFiltered = [];
    const selected = new Set(); // row keys, only meaningful when selectable

    function rowKeyOf(item) { return `${item.kind}:${item.data.id}`; }
    function getSelectedItems() {
      if (!selectable) return [];
      return items.filter((it) => selected.has(rowKeyOf(it)));
    }
    function notifySelection() {
      if (selectable && onSelectionChange) onSelectionChange(getSelectedItems());
    }

    function applyFilterStyles() {
      if (!filterBar) return;
      if (filterBar.tagName === "SELECT") {
        filterBar.value = filter;
      } else {
        filterBar.querySelectorAll(".filter-btn").forEach((b) => b.classList.toggle("active", b.dataset.filter === filter));
      }
    }
    function applyTypeFilterStyles() {
      if (!typeFilterBar) return;
      if (typeFilterBar.tagName === "SELECT") {
        typeFilterBar.value = typeFilter;
      } else {
        typeFilterBar.querySelectorAll(".filter-btn").forEach((b) => b.classList.toggle("active", b.dataset.typeFilter === typeFilter));
      }
    }

    if (filterBar) {
      if (filterBar.tagName === "SELECT") {
        filterBar.value = filter;
        filterBar.addEventListener("change", () => {
          filter = filterBar.value;
          page = 1;
          localStorage.setItem(filterKey, filter);
          render();
        });
      } else {
        filterBar.addEventListener("click", (e) => {
          const btn = e.target.closest(".filter-btn");
          if (!btn) return;
          filter = btn.dataset.filter;
          page = 1;
          localStorage.setItem(filterKey, filter);
          applyFilterStyles();
          render();
        });
        applyFilterStyles();
      }
    }
    if (typeFilterBar) {
      if (typeFilterBar.tagName === "SELECT") {
        typeFilterBar.value = typeFilter;
        typeFilterBar.addEventListener("change", () => {
          typeFilter = typeFilterBar.value;
          page = 1;
          localStorage.setItem(typeFilterKey, typeFilter);
          render();
        });
      } else {
        typeFilterBar.addEventListener("click", (e) => {
          const btn = e.target.closest(".filter-btn");
          if (!btn) return;
          typeFilter = btn.dataset.typeFilter;
          page = 1;
          localStorage.setItem(typeFilterKey, typeFilter);
          applyTypeFilterStyles();
          render();
        });
        applyTypeFilterStyles();
      }
    }

    pageSizeSelect.value = String(pageSize);
    pageSizeSelect.addEventListener("change", () => {
      pageSize = parseInt(pageSizeSelect.value, 10) || 20;
      page = 1;
      localStorage.setItem(pageSizeKey, String(pageSize));
      render();
    });
    prevBtn.addEventListener("click", () => { if (page > 1) { page -= 1; render(); } });
    nextBtn.addEventListener("click", () => { page += 1; render(); });

    function render() {
      const filtered = items.filter((item) => itemMatchesFilter(item, filter) && itemMatchesType(item, typeFilter));
      filtered.sort((a, b) => {
        const [ra, sa] = queueSortKey(a);
        const [rb, sb] = queueSortKey(b);
        return ra !== rb ? ra - rb : sa - sb;
      });
      lastFiltered = filtered;

      const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize));
      if (page > totalPages) page = totalPages;
      if (page < 1) page = 1;
      const start = (page - 1) * pageSize;
      const pageItems = filtered.slice(start, start + pageSize);

      tbody.innerHTML = "";
      if (pageItems.length === 0) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.colSpan = selectable ? 6 : 5;
        td.className = "hint";
        td.textContent = items.length === 0 ? emptyText : "No items match this filter.";
        tr.appendChild(td);
        tbody.appendChild(tr);
      } else {
        for (const item of pageItems) {
          const descriptor = buildRowDescriptor(item, rowMode);
          const key = rowKeyOf(item);
          tbody.appendChild(renderQueueRow(descriptor, {
            selectable,
            isSelected: () => selected.has(key),
            onToggleSelect: (checked) => {
              if (checked) selected.add(key); else selected.delete(key);
              notifySelection();
            },
            errorStatusEl,
          }));
        }
      }

      pageInfo.textContent = filtered.length === 0 ? "" : `Page ${page} of ${totalPages} (${filtered.length} item(s))`;
      prevBtn.disabled = page <= 1;
      nextBtn.disabled = page >= totalPages;
    }

    return {
      setItems(newItems) {
        items = newItems;
        if (selectable) {
          const validKeys = new Set(items.map(rowKeyOf));
          for (const k of Array.from(selected)) if (!validKeys.has(k)) selected.delete(k);
        }
        render();
        notifySelection();
      },
      getSelectedItems,
      selectAllFiltered() {
        for (const it of lastFiltered) if (it.data && it.kind === "task") selected.add(rowKeyOf(it));
        render();
        notifySelection();
      },
      clearSelection() { selected.clear(); render(); notifySelection(); },
      filteredCount() { return lastFiltered.length; },
    };
  }

  const activityView = createTableView({
    tbody: document.getElementById("task-table-body"),
    pageInfo: document.getElementById("queue-page-info"),
    prevBtn: document.getElementById("queue-prev-btn"),
    nextBtn: document.getElementById("queue-next-btn"),
    pageSizeSelect: document.getElementById("queue-page-size"),
    filterBar: document.getElementById("queue-filter-bar"),
    typeFilterBar: document.getElementById("queue-type-filter-bar"),
    filterKey: "febarr-queue-filter",
    typeFilterKey: "febarr-queue-type-filter",
    pageSizeKey: "febarr-queue-page-size",
    defaultFilter: "active",
    emptyText: "Nothing queued yet.",
    selectable: true,
    onSelectionChange: updateActivityBulkBar,
    errorStatusEl: queueStatusText,
  });

  // -- Downloads page -----------------------------------------------------
  // Its own live queue view, kept separate from Activity (which only
  // ever shows export tasks + analyze jobs + rejected titles now -- see
  // refreshTasks()' split below) -- same table-view machinery, own
  // filter/page/selection state and its own localStorage keys.
  const downloadsView = createTableView({
    tbody: document.getElementById("downloads-table-body"),
    pageInfo: document.getElementById("downloads-page-info"),
    prevBtn: document.getElementById("downloads-prev-btn"),
    nextBtn: document.getElementById("downloads-next-btn"),
    pageSizeSelect: document.getElementById("downloads-page-size"),
    filterBar: document.getElementById("downloads-filter-bar"),
    typeFilterBar: document.getElementById("downloads-type-filter-bar"),
    filterKey: "febarr-downloads-filter",
    typeFilterKey: "febarr-downloads-type-filter",
    pageSizeKey: "febarr-downloads-page-size",
    defaultFilter: "active",
    emptyText: "Nothing downloading yet.",
    selectable: true,
    onSelectionChange: updateDownloadsBulkBar,
    errorStatusEl: downloadsStatusText,
  });

  // -- Logs page --------------------------------------------------------
  // Every task/analyze-job's log, in one dedicated place -- same
  // underlying items as Activity (minus denied rows, which never have a
  // log), same table-view machinery, just read-only for analyze jobs;
  // a task row here gets its full management actions too (Resume/
  // Resync/Cancel), not just View log -- see buildRowDescriptor -- since
  // Activity/Downloads now drop a task the moment it's no longer queued/
  // running, this is the only place left a done/errored one is even
  // visible, let alone actionable.
  const logsView = createTableView({
    tbody: document.getElementById("logs-table-body"),
    pageInfo: document.getElementById("logs-page-info"),
    prevBtn: document.getElementById("logs-prev-btn"),
    nextBtn: document.getElementById("logs-next-btn"),
    pageSizeSelect: document.getElementById("logs-page-size"),
    filterBar: document.getElementById("logs-filter-bar"),
    typeFilterBar: document.getElementById("logs-type-filter-bar"),
    filterKey: "febarr-logs-filter",
    typeFilterKey: "febarr-logs-type-filter",
    pageSizeKey: "febarr-logs-page-size",
    defaultFilter: "active",
    emptyText: "No logs yet.",
    rowMode: "logs",
  });

  let logItemsCache = []; // latest task/analyze items, refreshed every poll -- see refreshTasks()
  let currentLogKey = null; // which item's log page-log-detail is currently showing

  const logDetailTitle = document.getElementById("log-detail-title");
  const logDetailMeta = document.getElementById("log-detail-meta");
  const logDetailContent = document.getElementById("log-detail-content");
  const logDetailBackBtn = document.getElementById("log-detail-back-btn");

  function renderLogDetail() {
    if (!currentLogKey) return;
    const item = logItemsCache.find((it) => `${it.kind}:${it.data.id}` === currentLogKey);
    if (!item) {
      logDetailTitle.textContent = "Gone";
      logDetailMeta.textContent = "This item no longer exists (removed or dismissed).";
      logDetailContent.textContent = "";
      return;
    }
    const descriptor = buildRowDescriptor(item, "logs");
    logDetailTitle.textContent = descriptor.title;
    logDetailMeta.textContent = [descriptor.statusLabel, descriptor.meta].filter(Boolean).join(" · ");
    const wasScrolledToBottom = logDetailContent.scrollTop + logDetailContent.clientHeight >= logDetailContent.scrollHeight - 4;
    logDetailContent.textContent = (descriptor.logLines || []).join("\n") || "(no log yet)";
    if (wasScrolledToBottom) logDetailContent.scrollTop = logDetailContent.scrollHeight;
  }

  function showLogDetail(key) {
    currentLogKey = key;
    showPage("log-detail");
    renderLogDetail();
  }

  logDetailBackBtn.addEventListener("click", () => {
    currentLogKey = null;
    showPage("logs");
  });

  // -- Links page ---------------------------------------------------------
  // The permanent record of every Febbox share link ever submitted for
  // Analyze (see febarr/links.py) -- every /api/shares/analyze call
  // (a fresh paste in the header search bar, or a Recheck click here)
  // saves/refreshes the matching row server-side, so this page just
  // lists what's already there and re-fires that same call. New
  // files/titles a Recheck finds land in Movies/Series exactly like a
  // first-time paste -- Activity is where its live progress shows, this
  // page only reflects once it's actually finished (last_checked_at).
  const linksTableBody = document.getElementById("links-table-body");
  const linksRefreshBtn = document.getElementById("links-refresh-btn");
  const linksStatus = document.getElementById("links-status");
  let linksCache = [];
  const linksRecheckingIds = new Set(); // optimistic "Checking..." until the next successful reload

  function fmtLinkTimestamp(iso) {
    if (!iso) return "Never";
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
  }

  function renderLinksTable() {
    linksTableBody.innerHTML = "";
    if (linksCache.length === 0) {
      const tr = document.createElement("tr");
      tr.innerHTML = '<td colspan="4" class="hint">No saved links yet -- paste a Febbox share link in the search bar to add one.</td>';
      linksTableBody.appendChild(tr);
      return;
    }
    for (const item of linksCache) {
      const tr = document.createElement("tr");

      const linkTd = document.createElement("td");
      const linkSpan = document.createElement("span");
      linkSpan.className = "ellipsis-cell";
      linkSpan.textContent = item.share_link;
      linkSpan.title = item.share_link;
      linkTd.appendChild(linkSpan);
      tr.appendChild(linkTd);

      const addedTd = document.createElement("td");
      addedTd.className = "hint";
      addedTd.textContent = fmtLinkTimestamp(item.added_at);
      tr.appendChild(addedTd);

      const checkedTd = document.createElement("td");
      checkedTd.className = "hint";
      checkedTd.textContent = linksRecheckingIds.has(item.id) ? "Checking..." : fmtLinkTimestamp(item.last_checked_at);
      tr.appendChild(checkedTd);

      const actionsTd = document.createElement("td");
      actionsTd.className = "q-actions-cell";
      const row = document.createElement("div");
      row.className = "row wrap q-actions";
      mountActionButtons(row, [
        {
          label: "Recheck", cls: "btn-ghost", disabled: linksRecheckingIds.has(item.id),
          title: "Re-run Analyze on this link -- new files/titles land in Movies/Series, progress shows in Activity",
          onClick: async () => {
            await api(`/api/links/${item.id}/recheck`, { method: "POST" });
            linksRecheckingIds.add(item.id);
            renderLinksTable();
            await refreshTasks();
          },
        },
        {
          label: "Remove", cls: "btn-danger",
          title: "Remove this saved link (doesn't touch anything already in your library)",
          onClick: async () => {
            await api(`/api/links/${item.id}`, { method: "DELETE" });
            await loadLinks();
          },
        },
      ], (err) => setStatus(linksStatus, err.message, true));
      actionsTd.appendChild(row);
      tr.appendChild(actionsTd);

      linksTableBody.appendChild(tr);
    }
  }

  async function loadLinks() {
    try {
      const items = await api("/api/links");
      // A link that just finished checking (real last_checked_at moved
      // past whatever we last saw) drops its optimistic "Checking...".
      for (const item of items) {
        const prev = linksCache.find((it) => it.id === item.id);
        if (linksRecheckingIds.has(item.id) && prev && item.last_checked_at !== prev.last_checked_at) {
          linksRecheckingIds.delete(item.id);
        }
      }
      linksCache = items.sort((a, b) => (b.last_submitted_at || "").localeCompare(a.last_submitted_at || ""));
      setStatus(linksStatus, "", false);
    } catch (err) {
      setStatus(linksStatus, `Couldn't load links: ${err.message}`, true);
    }
    renderLinksTable();
  }

  if (linksRefreshBtn) linksRefreshBtn.addEventListener("click", () => loadLinks());

  // Light poll while the page's actually open -- only matters for
  // catching a Recheck's real completion (last_checked_at), so it
  // doesn't need Activity's urgency; same throttled/in-flight-guarded
  // shape as refreshVisibleMediaList().
  const LINKS_POLL_INTERVAL_MS = 4000;
  const linksPollState = { lastRefreshAt: 0, inFlight: false };
  function refreshVisibleLinks() {
    if (!document.getElementById("page-links").classList.contains("active")) return;
    if (linksRecheckingIds.size === 0) return; // nothing pending -- no need to poll at all
    const now = Date.now();
    if (linksPollState.inFlight || now - linksPollState.lastRefreshAt < LINKS_POLL_INTERVAL_MS) return;
    linksPollState.inFlight = true;
    linksPollState.lastRefreshAt = now;
    loadLinks().finally(() => { linksPollState.inFlight = false; });
  }

  async function refreshTasks() {
    const [tasks, queueStatus, analyzeJobs] = await Promise.all([
      api("/api/tasks"),
      api("/api/tasks/queue_status"),
      api("/api/shares/analyze_jobs"),
    ]);

    queuePauseLabel.textContent = queueStatus.paused ? "Resume queue" : "Pause queue";
    queuePauseIcon.textContent = queueStatus.paused ? "play_arrow" : "pause";
    queuePauseBtn.dataset.paused = String(queueStatus.paused);
    if (downloadsPauseLabel) downloadsPauseLabel.textContent = queueStatus.paused ? "Resume queue" : "Pause queue";
    if (downloadsPauseIcon) downloadsPauseIcon.textContent = queueStatus.paused ? "play_arrow" : "pause";
    if (downloadsPauseBtn) downloadsPauseBtn.dataset.paused = String(queueStatus.paused);

    // If we're watching a share we asked to analyze, update the live
    // discovered/denied progress every poll -- not just once it finishes
    // -- since matches stream straight to the library as they're found.
    // Once it's actually finished (done/error/cancelled): if it left anything
    // denied behind, leave the job in Activity (tagged background, with
    // a Dismiss action) so those rows keep showing under the Rejected
    // tab until the user dismisses it themselves -- they're sourced
    // straight from the job, so deleting it early would wipe them out
    // on the very next poll. Nothing denied? Nothing worth keeping
    // around, so it's cleaned up immediately same as before.
    let visibleAnalyzeJobs = analyzeJobs;
    if (pendingAnalyzeJobId) {
      const job = analyzeJobs.find((j) => j.id === pendingAnalyzeJobId);
      if (job && job.status !== "running") {
        pendingAnalyzeJobId = null;
        if (job.denied.length === 0) {
          try {
            await api(`/api/shares/analyze/${job.id}`, { method: "DELETE" });
          } catch (err) {
            // not fatal -- it'll just show in Activity until the next dismiss/poll
          }
          visibleAnalyzeJobs = analyzeJobs.filter((j) => j.id !== job.id);
        }
      }
    }

    // Each denied group from every still-visible analyze job becomes its
    // own row too, filterable via the Rejected tab -- flattened out of
    // the job here rather than kept nested, same as everything else in
    // this table.
    const deniedItems = [];
    for (const job of visibleAnalyzeJobs) {
      (job.denied || []).forEach((d, index) => {
        deniedItems.push({ kind: "denied", data: { ...d, job_id: job.id, index } });
      });
    }

    // Activity/Downloads only ever show a task while it's actually
    // queued or running -- the moment it settles (done/error/cancelled)
    // it drops off immediately rather than lingering until someone
    // switches to a "Completed"/"All" filter. Logs (below, built from
    // the unfiltered `tasks`) is the complete history from here on.
    const liveTasks = tasks.filter((t) => t.status === "queued" || t.status === "running");
    const exportTasks = liveTasks.filter((t) => t.task_type !== "download");
    const downloadTasks = liveTasks.filter((t) => t.task_type === "download");

    // Speed/ETA tracking: fed exactly once per poll tick, from this
    // fresh server snapshot -- see updateDownloadRate()'s docstring for
    // why it can't just be recomputed inside buildRowDescriptor/
    // renderDownloadTaskPanel on every render. Stale entries (a
    // download that finished, errored, or was removed since the last
    // poll) are pruned so the map doesn't grow across a long session.
    const liveDownloadIds = new Set(downloadTasks.map((t) => t.id));
    for (const k of Array.from(downloadRateState.keys())) {
      if (!liveDownloadIds.has(k)) downloadRateState.delete(k);
    }
    for (const t of downloadTasks) {
      if (t.status === "running") updateDownloadRate(t.id, downloadByteTotals(t).bytesDone);
    }

    activityView.setItems([
      ...exportTasks.map((t) => ({ kind: "task", data: t })),
      ...visibleAnalyzeJobs.map((j) => ({ kind: "analyze", data: j })),
      ...deniedItems,
    ]);
    downloadsView.setItems(downloadTasks.map((t) => ({ kind: "task", data: t })));

    // Logs page: every task regardless of status (done/error/cancelled
    // included -- this is now the only place they're visible/actionable,
    // see buildRowDescriptor's logs-mode branch) + analyze jobs, minus
    // denied rows (they never have a log of their own -- see
    // buildRowDescriptor's denied branch).
    logItemsCache = [
      ...tasks.map((t) => ({ kind: "task", data: t })),
      ...visibleAnalyzeJobs.map((j) => ({ kind: "analyze", data: j })),
    ];
    logsView.setItems(logItemsCache);
    if (document.getElementById("page-log-detail").classList.contains("active")) renderLogDetail();
  }

  async function refreshAll() {
    await refreshTasks();
  }

  // Movies/Series aren't otherwise live -- reload whichever of the two is
  // actually on screen periodically too, so a discovered item's "Queue"
  // -> exporting -> ready progression shows up without the user having
  // to leave and come back. Deliberately NOT on every poll tick, though,
  // and NOT awaited from poll() itself: /api/media walks every title's
  // folder on disk to count its files (see media_library.list_media()),
  // which is fine once on a page visit but far too slow to redo every
  // 1.5s against a library of thousands -- doing that either way ties up
  // the main task-polling loop behind it (queue status/Activity go
  // stale) and, piled on top of a large bulk action's own requests, was
  // enough to start dropping connections ("Failed to fetch") server-side.
  // The in-flight guard skips a tick outright if the last request for
  // that type hasn't returned yet, rather than queuing up more on top.
  const MEDIA_POLL_INTERVAL_MS = 20000;
  const mediaPollState = {
    movies: { lastRefreshAt: 0, inFlight: false },
    series: { lastRefreshAt: 0, inFlight: false },
  };

  function refreshVisibleMediaList() {
    const now = Date.now();
    for (const type of ["movies", "series"]) {
      if (!document.getElementById(`page-${type}`).classList.contains("active")) continue;
      const state = mediaPollState[type];
      if (state.inFlight || now - state.lastRefreshAt < MEDIA_POLL_INTERVAL_MS) continue;
      state.inFlight = true;
      state.lastRefreshAt = now;
      loadMediaList(type).finally(() => { state.inFlight = false; });
    }
  }

  // Used for a deliberate, immediate load (switching to the page, right
  // after a row/bulk action) -- stamps lastRefreshAt too, so the next
  // poll tick doesn't immediately redo the same expensive fetch.
  function loadMediaListNow(type) {
    const state = mediaPollState[type];
    state.lastRefreshAt = Date.now();
    state.inFlight = true;
    return loadMediaList(type).finally(() => { state.inFlight = false; });
  }

  async function poll() {
    try {
      await refreshTasks();
    } catch (err) {
      if (queueStatusText) queueStatusText.textContent = `Error refreshing: ${err.message}`;
    }
    refreshVisibleMediaList(); // fire-and-forget -- see comment above
    refreshVisibleMediaDetail(); // ditto -- keeps an open detail page's download progress live
    refreshVisibleLinks(); // ditto -- only actually fetches while a Recheck is pending
    setTimeout(poll, POLL_MS);
  }

  // -- Movies / Series library ---------------------------------------------
  // Shows a title through its whole lifecycle -- discovered (Analyze
  // found it, nobody's queued it), queued/running (an active export
  // task), error (a task that failed on its own), ready (finished,
  // sitting on disk) -- see media_library.augment_with_pipeline() for
  // how the server assembles that. There's no "cancelled" status: Cancel
  // (available here now, not just Activity) settles a task straight
  // back into "discovered" the moment it actually stops, so a row never
  // sits around in some cancelled limbo -- see tasks.TaskManager.
  // on_settled. Row actions and the bulk bar below both key off
  // item.status the same way; has_folder gates View (nothing to view
  // until an export has actually written something). "Remove" is this
  // page's one hard-delete action -- it deletes any exported files
  // *and* whatever queue/library record exists, together, so there's no
  // separate "Delete" button -- available for anything short of
  // "running" (cancel it first, same rule TaskManager.remove_task()
  // enforces).
  const MEDIA_STATUS_LABELS = {
    discovered: "Discovered", queued: "Queued", running: "Exporting", ready: "Ready", error: "Error",
    download_queued: "Queued", downloading: "Downloading", download_error: "Download error",
    downloaded: "Downloaded",
  };
  const moviesTableBody = document.getElementById("movies-table-body");
  const moviesCount = document.getElementById("movies-count");
  const seriesTableBody = document.getElementById("series-table-body");
  const seriesCount = document.getElementById("series-count");
  const moviesStatusFilterBar = document.getElementById("movies-status-filter-bar");
  const seriesStatusFilterBar = document.getElementById("series-status-filter-bar");
  const moviesStatusEl = document.getElementById("movies-status");
  const seriesStatusEl = document.getElementById("series-status");

  // Row/bulk-action feedback (errors, "nothing eligible", bulk result
  // summaries) goes here instead of a browser alert() -- same toolbar
  // hint-line pattern the rest of the app already uses.
  function mediaStatus(type, message, isError) {
    setStatus(type === "movies" ? moviesStatusEl : seriesStatusEl, message, isError);
  }

  const mediaListCache = { movies: [], series: [] };
  const mediaFilteredCache = { movies: [], series: [] }; // last render()'s filtered set, for "select all filtered"
  const mediaSelection = { movies: new Set(), series: new Set() }; // selected media ids, session-only
  const mediaStatusFilter = {
    movies: localStorage.getItem("febarr-movies-status-filter") || "all",
    series: localStorage.getItem("febarr-series-status-filter") || "all",
  };

  // -- customizable columns -------------------------------------------------
  // Every field the table can show. `key` is what's persisted (visible
  // columns, their order, and their widths, per type, in localStorage);
  // `sortKey` returns a comparable value for header-click sorting;
  // `render` returns the cell's text. Deliberately text-only (no nested
  // markup) except "status", which the row-builder below wraps in the
  // same colored status-badge span used everywhere else -- keeping every
  // other column a plain string is what lets the header and every body
  // cell share one code path instead of each column needing its own.
  function mediaColumnDefs(type) {
    const filesLabel = type === "movies" ? "Files" : "Episodes";
    return [
      { key: "status", label: "Status", width: 110,
        sortKey: (it) => MEDIA_STATUS_LABELS[it.status] || it.status || "",
        render: (it) => MEDIA_STATUS_LABELS[it.status] || fmtStatus(it.status) },
      { key: "title", label: "Title", width: 260,
        sortKey: (it) => (it.title || "").toLowerCase(), render: (it) => it.title },
      { key: "year", label: "Year", width: 80,
        sortKey: (it) => it.year || 0, render: (it) => it.year || "" },
      { key: "files", label: filesLabel, width: 90,
        sortKey: (it) => it.file_count || 0,
        render: (it) => (it.status === "queued" || it.status === "running")
          ? `${it.file_count || 0} so far` : String(it.file_count) },
      { key: "category", label: "Category", width: 130,
        sortKey: (it) => CATEGORY_LABELS[it.category] || it.category || "",
        render: (it) => CATEGORY_LABELS[it.category] || it.category || "" },
      { key: "progress", label: "Progress", width: 130,
        sortKey: (it) => {
          const p = it.task_progress;
          return p && p.total_items ? p.files_written / p.total_items : -1;
        },
        render: (it) => {
          const p = it.task_progress;
          if (!p) return "";
          if (p.phase === "discovering") return "Discovering…";
          return p.total_items ? `${p.files_written}/${p.total_items}` : "";
        } },
      { key: "current_item", label: "Current file", width: 220,
        sortKey: (it) => (it.task_progress && it.task_progress.current_item) || "",
        render: (it) => (it.task_progress && it.task_progress.current_item) || "" },
      { key: "on_disk", label: "On disk", width: 80,
        sortKey: (it) => (it.has_folder ? 1 : 0), render: (it) => (it.has_folder ? "Yes" : "No") },
      { key: "path", label: "Path", width: 280,
        sortKey: (it) => it.id, render: (it) => it.id },
    ];
  }
  const MEDIA_DEFAULT_COLUMNS = ["status", "title", "category", "year", "files"];

  function loadMediaColumnState(type) {
    try {
      const parsed = JSON.parse(localStorage.getItem(`febarr-${type}-columns`));
      if (Array.isArray(parsed) && parsed.length) return parsed;
    } catch (e) { /* fall through to defaults */ }
    return MEDIA_DEFAULT_COLUMNS.map((key) => ({ key, width: null }));
  }
  function saveMediaColumnState(type) {
    localStorage.setItem(`febarr-${type}-columns`, JSON.stringify(mediaColumnState[type]));
  }
  const mediaColumnState = { movies: loadMediaColumnState("movies"), series: loadMediaColumnState("series") };

  function loadMediaSortState(type) {
    try {
      const parsed = JSON.parse(localStorage.getItem(`febarr-${type}-sort`));
      if (parsed && parsed.key) return parsed;
    } catch (e) { /* fall through to default */ }
    return { key: "title", dir: "asc" };
  }
  function saveMediaSortState(type) {
    localStorage.setItem(`febarr-${type}-sort`, JSON.stringify(mediaSortState[type]));
  }
  const mediaSortState = { movies: loadMediaSortState("movies"), series: loadMediaSortState("series") };

  function sortMediaItems(items, type) {
    const defs = mediaColumnDefs(type);
    const state = mediaSortState[type];
    const def = defs.find((d) => d.key === state.key) || defs.find((d) => d.key === "title");
    const mul = state.dir === "desc" ? -1 : 1;
    const sorted = items.slice();
    sorted.sort((a, b) => {
      const av = def.sortKey(a);
      const bv = def.sortKey(b);
      if (av < bv) return -1 * mul;
      if (av > bv) return 1 * mul;
      return (a.title || "").toLowerCase().localeCompare((b.title || "").toLowerCase());
    });
    return sorted;
  }

  // Resize: drag the handle on a header's right edge. Reorder: drag the
  // header itself (anywhere except the handle) and drop it on another
  // one. Both just mutate mediaColumnState[type] and persist -- resize
  // updates the live <col> width directly for a smooth drag, reorder
  // rebuilds the whole head since the column order actually changed.
  function wireColumnResize(handle, colEl, type, getIndex) {
    handle.addEventListener("mousedown", (e) => {
      e.preventDefault();
      e.stopPropagation();
      const startX = e.clientX;
      const startWidth = colEl.offsetWidth;
      handle.classList.add("resizing");
      function onMove(ev) {
        colEl.style.width = `${Math.max(50, startWidth + (ev.clientX - startX))}px`;
      }
      function onUp() {
        handle.classList.remove("resizing");
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        const idx = getIndex();
        if (idx >= 0) {
          mediaColumnState[type][idx].width = colEl.offsetWidth;
          saveMediaColumnState(type);
        }
      }
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    });
  }
  function wireColumnDrag(th, handle, type, getIndex) {
    th.addEventListener("dragstart", (e) => {
      if (e.target === handle) { e.preventDefault(); return; }
      e.dataTransfer.effectAllowed = "move";
      e.dataTransfer.setData("text/plain", String(getIndex()));
    });
    th.addEventListener("dragover", (e) => { e.preventDefault(); th.classList.add("drag-over"); });
    th.addEventListener("dragleave", () => th.classList.remove("drag-over"));
    th.addEventListener("drop", (e) => {
      e.preventDefault();
      th.classList.remove("drag-over");
      const fromIndex = parseInt(e.dataTransfer.getData("text/plain"), 10);
      const toIndex = getIndex();
      if (Number.isNaN(fromIndex) || fromIndex === toIndex) return;
      const cols = mediaColumnState[type];
      const [moved] = cols.splice(fromIndex, 1);
      cols.splice(toIndex, 0, moved);
      saveMediaColumnState(type);
      renderMediaTableHead(type);
      renderMediaList(type);
    });
  }

  function wireMediaSelectAllCheckbox(type, cb) {
    cb.addEventListener("change", () => {
      if (cb.checked) mediaFilteredCache[type].forEach((it) => mediaSelection[type].add(it.id));
      else mediaSelection[type].clear();
      renderMediaList(type);
    });
  }

  // Rebuilds the <colgroup> + header row from mediaColumnState/
  // mediaSortState -- called on load and whenever columns/sort actually
  // change (not on every poll-driven body refresh, so an in-progress
  // resize/drag and the picker's open state aren't disturbed by that).
  function renderMediaTableHead(type) {
    const theadRow = document.getElementById(`${type}-thead-row`);
    const colgroup = document.getElementById(`${type}-colgroup`);
    const defs = mediaColumnDefs(type);
    theadRow.innerHTML = "";
    colgroup.innerHTML = "";

    const selectCol = document.createElement("col");
    selectCol.className = "q-select-col";
    selectCol.style.width = "32px";
    colgroup.appendChild(selectCol);
    const selectTh = document.createElement("th");
    selectTh.className = "q-select-cell";
    const selectAllCb = document.createElement("input");
    selectAllCb.type = "checkbox";
    selectAllCb.id = `${type}-select-all`;
    selectAllCb.title = "Select all filtered";
    wireMediaSelectAllCheckbox(type, selectAllCb);
    selectTh.appendChild(selectAllCb);
    theadRow.appendChild(selectTh);

    mediaColumnState[type].forEach((colState, index) => {
      const def = defs.find((d) => d.key === colState.key);
      if (!def) return;
      const col = document.createElement("col");
      col.style.width = `${colState.width || def.width}px`;
      colgroup.appendChild(col);

      const th = document.createElement("th");
      th.className = "col-th";
      th.draggable = true;
      th.title = "Click to sort, drag to reorder";
      th.appendChild(document.createTextNode(def.label));
      if (mediaSortState[type].key === def.key) {
        const indicator = document.createElement("span");
        indicator.className = "material-symbols-outlined sort-indicator";
        indicator.textContent = mediaSortState[type].dir === "asc" ? "arrow_upward" : "arrow_downward";
        th.appendChild(indicator);
      }
      const handle = document.createElement("span");
      handle.className = "col-resize-handle";
      th.appendChild(handle);

      th.addEventListener("click", (e) => {
        if (e.target === handle) return;
        const s = mediaSortState[type];
        if (s.key === def.key) s.dir = s.dir === "asc" ? "desc" : "asc";
        else { s.key = def.key; s.dir = "asc"; }
        saveMediaSortState(type);
        const gridSortSelect = document.getElementById(`${type}-grid-sort`);
        if (gridSortSelect && GRID_SORT_KEYS.includes(s.key)) gridSortSelect.value = s.key;
        renderMediaTableHead(type);
        renderMediaList(type);
      });
      wireColumnResize(handle, col, type, () => mediaColumnState[type].indexOf(colState));
      wireColumnDrag(th, handle, type, () => mediaColumnState[type].indexOf(colState));

      theadRow.appendChild(th);
    });

    const actionsCol = document.createElement("col"); // no explicit width -- absorbs remaining space
    colgroup.appendChild(actionsCol);
    theadRow.appendChild(document.createElement("th"));
  }

  function renderColumnPicker(type) {
    const picker = document.getElementById(`${type}-column-picker`);
    const visibleKeys = new Set(mediaColumnState[type].map((c) => c.key));
    picker.innerHTML = "";
    for (const def of mediaColumnDefs(type)) {
      const label = document.createElement("label");
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = visibleKeys.has(def.key);
      cb.addEventListener("change", () => {
        if (cb.checked) {
          mediaColumnState[type].push({ key: def.key, width: null });
        } else {
          mediaColumnState[type] = mediaColumnState[type].filter((c) => c.key !== def.key);
        }
        saveMediaColumnState(type);
        renderMediaTableHead(type);
        renderMediaList(type);
      });
      label.appendChild(cb);
      label.appendChild(document.createTextNode(def.label));
      picker.appendChild(label);
    }
  }
  function wireColumnPickerToggle(type) {
    const btn = document.getElementById(`${type}-columns-btn`);
    const picker = document.getElementById(`${type}-column-picker`);
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const wasHidden = picker.classList.contains("hidden");
      document.querySelectorAll(".column-picker").forEach((p) => p.classList.add("hidden"));
      if (wasHidden) {
        renderColumnPicker(type);
        picker.classList.remove("hidden");
      }
    });
  }
  document.addEventListener("click", (e) => {
    document.querySelectorAll(".column-picker").forEach((p) => {
      if (!p.classList.contains("hidden") && !p.contains(e.target) && !e.target.closest(".column-picker-wrap")) {
        p.classList.add("hidden");
      }
    });
  });

  // Remove = delete any exported files (has_folder) plus whatever queue/
  // library record exists (a discovered item, or a task -- a row is
  // never both at once). Doesn't reload the list itself -- row actions
  // and the bulk "remove" both do that once, after all their calls
  // settle, instead of each item triggering its own reload.
  async function removeMediaItem(item, type) {
    if (item.has_folder) {
      await api(`/api/media/${mediaIdToUrl(item.id)}`, { method: "DELETE" });
    }
    if (item.status === "discovered") {
      await api(`/api/discovered/${item.discovered_id}`, { method: "DELETE" });
    } else if (item.task_id) {
      await api(`/api/tasks/${item.task_id}/remove`, { method: "POST" });
    }
  }

  // What actions make sense for one row, given its pipeline status --
  // reused as-is for the bulk bar's own eligibility checks further down.
  // "download_queued"/"downloading"/"download_error" are Download's own
  // pipeline states, distinct from the export ones but handled the same
  // shape -- see media_library.augment_with_pipeline()'s download-overlay
  // loop for where they come from.
  function mediaRowActions(item, type) {
    const actions = [];
    if (item.status === "discovered") {
      actions.push({ label: "Queue", cls: "btn-primary", onClick: async () => {
        await api(`/api/discovered/${item.discovered_id}/queue`, { method: "POST" });
        await loadMediaList(type);
      } });
    } else if (item.status === "queued" || item.status === "running"
            || item.status === "download_queued" || item.status === "downloading") {
      actions.push({ label: "Cancel", cls: "btn-danger", onClick: async () => {
        await api(`/api/tasks/${item.task_id}/cancel`, { method: "POST" });
        await loadMediaList(type);
      } });
    } else if ((item.status === "ready" || item.status === "downloaded") && (item.streamable_count || 0) > 0) {
      actions.push({ label: "Download", cls: "btn-primary", onClick: async () => {
        await api(`/api/media/${mediaIdToUrl(item.id)}/download`, { method: "POST" });
        await loadMediaList(type);
      } });
    }
    if (item.status !== "running" && item.status !== "downloading") {
      actions.push({ label: "Remove", cls: "btn-danger", onClick: async () => {
        await removeMediaItem(item, type);
        await loadMediaList(type);
      } });
    }
    if (item.has_folder) {
      actions.push({ label: "View", cls: "btn-ghost", onClick: () => showMediaDetail(item.id, type) });
    }
    return actions;
  }

  function renderMediaTableBody(type, filtered) {
    const tbody = type === "movies" ? moviesTableBody : seriesTableBody;
    const defs = mediaColumnDefs(type);
    const colCount = mediaColumnState[type].length + 2; // + select + actions

    tbody.innerHTML = "";
    if (filtered.length === 0) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = colCount;
      td.className = "hint";
      td.textContent = mediaListCache[type].length === 0
        ? `No ${type} found under the export root.`
        : "No matches.";
      tr.appendChild(td);
      tbody.appendChild(tr);
      return;
    }
    for (const item of filtered) {
      const tr = document.createElement("tr");
      tr.className = "queue-row clickable-row";
      tr.addEventListener("click", (e) => {
        if (e.target.closest("input, button, a, .q-actions, .q-select-cell")) return;
        showMediaDetail(item.id, type);
      });

      const selectTd = document.createElement("td");
      selectTd.className = "q-select-cell";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = mediaSelection[type].has(item.id);
      cb.addEventListener("change", () => {
        if (cb.checked) mediaSelection[type].add(item.id); else mediaSelection[type].delete(item.id);
        updateMediaBulkBar(type);
      });
      selectTd.appendChild(cb);
      tr.appendChild(selectTd);

      for (const colState of mediaColumnState[type]) {
        const def = defs.find((d) => d.key === colState.key);
        if (!def) continue;
        const td = document.createElement("td");
        if (def.key === "title") {
          td.className = "q-title";
          td.textContent = def.render(item, type);
        } else if (def.key === "status") {
          const badge = document.createElement("span");
          badge.className = `status-badge status-${item.status}`;
          badge.textContent = def.render(item, type);
          td.appendChild(badge);
        } else {
          td.textContent = def.render(item, type);
        }
        tr.appendChild(td);
      }

      const actionsTd = document.createElement("td");
      actionsTd.className = "q-actions-cell";
      const actionsRow = document.createElement("div");
      actionsRow.className = "row wrap q-actions";
      mountActionButtons(actionsRow, mediaRowActions(item, type), (err) => mediaStatus(type, err.message, true));
      actionsTd.appendChild(actionsRow);
      tr.appendChild(actionsTd);

      tbody.appendChild(tr);
    }
  }

  // -- poster lookup for grid view -------------------------------------
  // Reuses the existing TMDB search endpoint (same one the header search
  // bar's plain-title lookup uses) rather than a dedicated backend
  // route -- one lookup per distinct title/year/tmdb_id, cached client-side for
  // the session so switching views/re-sorting never re-fetches.
  const POSTER_BASE_GRID = "https://image.tmdb.org/t/p/w185";
  const mediaPosterCache = { movies: new Map(), series: new Map() }; // key -> Promise<poster_path|null>
  function posterCacheKey(item) {
    if (item.tmdb_id) return `tmdb:${item.tmdb_id}`;
    return `${item.title}|${item.year || ""}`;
  }
  function resolvePoster(item, type) {
    const key = posterCacheKey(item);
    const cache = mediaPosterCache[type];
    if (cache.has(key)) return cache.get(key);
    if (item.poster_path) {
      const p = Promise.resolve(item.poster_path);
      cache.set(key, p);
      return p;
    }
    const cleanTitle = (item.title || "").trim();
    if (!cleanTitle) {
      const p = Promise.resolve(null);
      cache.set(key, p);
      return p;
    }
    const promise = api(`/api/tmdb/search?q=${encodeURIComponent(cleanTitle)}`)
      .then((data) => {
        const results = data.results || [];
        if (!results.length) return null;
        let match = null;
        if (item.tmdb_id) {
          match = results.find((r) => r.tmdb_id === item.tmdb_id && r.poster_path);
        }
        if (!match && item.year) {
          match = results.find((r) => r.year && String(r.year) === String(item.year) && r.poster_path);
        }
        if (!match) {
          match = results.find((r) => r.poster_path) || results[0];
        }
        return (match && match.poster_path) || null;
      })
      .catch(() => null);
    cache.set(key, promise);
    return promise;
  }
  function applyCardPoster(img, fallback, item, type) {
    resolvePoster(item, type).then((posterPath) => {
      if (!posterPath) return;
      const fullUrl = posterPath.startsWith("http") ? posterPath : POSTER_BASE_GRID + posterPath;
      img.onload = () => {
        img.classList.remove("hidden");
        fallback.classList.add("hidden");
      };
      img.onerror = () => {
        img.classList.add("hidden");
        fallback.classList.remove("hidden");
      };
      img.src = fullUrl;
      if (img.complete && img.naturalWidth > 0) {
        img.classList.remove("hidden");
        fallback.classList.add("hidden");
      }
    });
  }

  // Grid card status line: pre-export statuses (nothing to download yet)
  // keep the plain status label; once a title has actually exported --
  // ready/downloaded/downloading/download_queued/download_error, all of
  // which carry a real file_count -- show progress instead ("x/x
  // downloaded", or "downloading" while a download task's running),
  // same shape as the detail page header (see renderMediaDetail).
  const MEDIA_CARD_PROGRESS_STATUSES = new Set(["ready", "downloaded", "downloading", "download_queued", "download_error"]);
  function mediaCardStatusText(item) {
    if (!MEDIA_CARD_PROGRESS_STATUSES.has(item.status)) {
      return MEDIA_STATUS_LABELS[item.status] || fmtStatus(item.status);
    }
    const total = item.file_count || 0;
    const downloadedNow = item.downloaded_count || 0;
    const downloading = item.status === "downloading" || item.status === "download_queued";
    return `${downloadedNow}/${total} ${downloading ? "downloading" : "downloaded"}`;
  }

  function renderMediaGrid(type, filtered) {
    const grid = document.getElementById(`${type}-grid`);
    grid.innerHTML = "";
    if (filtered.length === 0) {
      const empty = document.createElement("p");
      empty.className = "hint media-grid-empty";
      empty.textContent = mediaListCache[type].length === 0
        ? `No ${type} found under the export root.`
        : "No matches.";
      grid.appendChild(empty);
      return;
    }
    const filesWord = type === "movies" ? "files" : "episodes";
    for (const item of filtered) {
      const card = document.createElement("div");
      card.className = "media-card clickable-card";
      card.classList.toggle("selected", mediaSelection[type].has(item.id));
      card.addEventListener("click", (e) => {
        if (e.target.closest("input, button, a, .media-card-actions, .media-card-select")) return;
        showMediaDetail(item.id, type);
      });

      const selectWrap = document.createElement("div");
      selectWrap.className = "media-card-select";
      const cb = document.createElement("input");
      cb.type = "checkbox";
      cb.checked = mediaSelection[type].has(item.id);
      cb.addEventListener("change", () => {
        if (cb.checked) mediaSelection[type].add(item.id); else mediaSelection[type].delete(item.id);
        card.classList.toggle("selected", cb.checked);
        updateMediaBulkBar(type);
      });
      selectWrap.appendChild(cb);

      const posterWrap = document.createElement("div");
      posterWrap.className = "media-card-poster";
      const img = document.createElement("img");
      img.alt = item.title || "";
      // Not loading="lazy": the poster starts hidden (display:none, via
      // the "hidden" class below) until it loads, and a display:none
      // image never intersects the viewport -- native lazy-loading would
      // defer the fetch forever, so onload/onerror (which is what
      // removes "hidden") would never fire and every card would be
      // stuck showing its title-only fallback permanently.
      img.classList.add("hidden");
      const fallback = document.createElement("div");
      fallback.className = "media-card-poster-fallback";
      fallback.textContent = item.title;
      posterWrap.appendChild(img);
      posterWrap.appendChild(fallback);
      applyCardPoster(img, fallback, item, type);

      // Badge removed; status will be displayed in title

      const body = document.createElement("div");
      body.className = "media-card-body";
      const statusEl = document.createElement("div");
      statusEl.className = "media-card-status";
      statusEl.textContent = mediaCardStatusText(item);
      const metaEl = document.createElement("div");
      metaEl.className = "media-card-meta";
      metaEl.textContent = (item.status === "queued" || item.status === "running")
        ? `${item.file_count || 0} ${filesWord} so far`
        : `${item.file_count} ${filesWord}`;
      const actionsEl = document.createElement("div");
      actionsEl.className = "media-card-actions";
      mountActionButtons(actionsEl, mediaRowActions(item, type), (err) => mediaStatus(type, err.message, true));

      body.appendChild(statusEl);
      body.appendChild(metaEl);
      body.appendChild(actionsEl);

      card.appendChild(selectWrap);
      card.appendChild(posterWrap);
      // badge omitted
      card.appendChild(body);
      grid.appendChild(card);
    }
  }

  function loadMediaViewMode(type) {
    return localStorage.getItem(`febarr-${type}-view`) === "grid" ? "grid" : "table";
  }
  const mediaViewMode = { movies: loadMediaViewMode("movies"), series: loadMediaViewMode("series") };

  function renderMediaList(type) {
    const countEl = type === "movies" ? moviesCount : seriesCount;
    const all = mediaListCache[type];
    let filtered = all;
    if (mediaStatusFilter[type] !== "all") {
      filtered = filtered.filter((m) => {
        if (mediaStatusFilter[type] === "downloaded") {
          return m.status === "downloaded" || (m.downloaded_count > 0 && (m.status === "ready" || m.status === "downloaded"));
        }
        if (mediaStatusFilter[type] === "downloading") {
          return m.status === "downloading" || m.status === "download_queued";
        }
        if (mediaStatusFilter[type] === "ready") {
          return m.status === "ready";
        }
        return m.status === mediaStatusFilter[type];
      });
    }
    filtered = sortMediaItems(filtered, type);
    mediaFilteredCache[type] = filtered;

    const tableScroll = document.querySelector(`#page-${type} .table-scroll`);
    const grid = document.getElementById(`${type}-grid`);
    if (mediaViewMode[type] === "grid") {
      tableScroll.classList.add("hidden");
      grid.classList.remove("hidden");
      renderMediaGrid(type, filtered);
    } else {
      grid.classList.add("hidden");
      tableScroll.classList.remove("hidden");
      renderMediaTableBody(type, filtered);
    }

    const pending = all.filter((m) => m.status !== "ready" && m.status !== "downloaded").length;
    const noun = `${type === "movies" ? "movie" : "series"}${type === "movies" ? "s" : ""}`;
    let text = filtered.length !== all.length
      ? `${filtered.length} of ${all.length} ${noun}`
      : `${all.length} ${noun}`;
    if (pending) text += ` (${pending} pending)`;
    if (countEl) countEl.textContent = text;
    updateMediaBulkBar(type);
  }

  // Table view sorts by clicking a header; grid view has no headers, so
  // it gets its own small field+direction control instead, sharing the
  // exact same mediaSortState/sortMediaItems() -- only the columns
  // simple enough to make sense without a table (status/title/category/
  // year/files) are offered here, though switching back to table mode still
  // honors whatever richer sort a header click set.
  const GRID_SORT_KEYS = ["status", "title", "category", "year", "files"];
  function wireMediaViewToggle(type) {
    const viewSelect = document.getElementById(`${type}-view-select`);
    const columnsWrap = document.getElementById(`${type}-columns-wrap`);
    const gridSortSelect = document.getElementById(`${type}-grid-sort`);

    function applyViewUI() {
      const mode = mediaViewMode[type];
      if (viewSelect) viewSelect.value = mode;
      if (columnsWrap) columnsWrap.classList.toggle("hidden", mode !== "table");
      if (gridSortSelect) {
        gridSortSelect.value = GRID_SORT_KEYS.includes(mediaSortState[type].key) ? mediaSortState[type].key : "title";
      }
    }
    applyViewUI();

    if (viewSelect) {
      viewSelect.addEventListener("change", () => {
        mediaViewMode[type] = viewSelect.value;
        localStorage.setItem(`febarr-${type}-view`, viewSelect.value);
        applyViewUI();
        renderMediaList(type);
      });
    }
    if (gridSortSelect) {
      gridSortSelect.addEventListener("change", () => {
        mediaSortState[type].key = gridSortSelect.value;
        saveMediaSortState(type);
        renderMediaList(type);
      });
    }
  }

  async function loadMediaList(type) {
    const qsType = type === "movies" ? "movie" : "series";
    try {
      mediaListCache[type] = await api(`/api/media?type=${qsType}`);
      mediaStatus(type, "", false);
    } catch (err) {
      // Keep showing whatever was last loaded successfully instead of
      // wiping it to an empty list -- a slow/failed request on a big
      // library used to silently render as "0 movies found", which
      // reads as an empty library instead of what it actually was: a
      // request that didn't come back.
      mediaStatus(type, `Couldn't refresh: ${err.message}`, true);
      return;
    }
    const validIds = new Set(mediaListCache[type].map((m) => m.id));
    for (const id of Array.from(mediaSelection[type])) if (!validIds.has(id)) mediaSelection[type].delete(id);
    renderMediaList(type);
  }

  function wireMediaStatusFilterSelect(selectEl, type) {
    if (!selectEl) return;
    const saved = mediaStatusFilter[type];
    if (saved && Array.from(selectEl.options).some((opt) => opt.value === saved)) {
      selectEl.value = saved;
    } else {
      selectEl.value = "all";
      mediaStatusFilter[type] = "all";
    }
    selectEl.addEventListener("change", () => {
      mediaStatusFilter[type] = selectEl.value;
      localStorage.setItem(`febarr-${type}-status-filter`, selectEl.value);
      renderMediaList(type);
    });
  }
  wireMediaStatusFilterSelect(moviesStatusFilterBar, "movies");
  wireMediaStatusFilterSelect(seriesStatusFilterBar, "series");

  // -- Movies / Series bulk actions ----------------------------------------
  // Same shape as Activity's bulk bar: a selection Set keyed by media id,
  // a header checkbox that selects/clears every currently-filtered row,
  // and buttons that apply to whichever selected rows are eligible for
  // that action (ineligible ones are just skipped, and the summary
  // afterwards says how many actually ran).
  const mediaSelectionMode = { movies: false, series: false };

  function updateMediaBulkBar(type) {
    const bar = document.getElementById(`${type}-bulk-bar`);
    const countEl = document.getElementById(`${type}-bulk-count`);
    const selectAllCb = document.getElementById(`${type}-select-all`);
    const selectBtn = document.getElementById(`${type}-select-btn`);
    const pageEl = document.getElementById(`page-${type}`);
    const n = mediaSelection[type].size;
    const isMode = mediaSelectionMode[type] || n > 0;

    if (pageEl) pageEl.classList.toggle("selection-active", isMode);
    if (selectBtn) selectBtn.classList.toggle("active", isMode);
    bar.classList.toggle("hidden", !isMode);
    countEl.textContent = `${n} selected`;
    const filteredCount = mediaFilteredCache[type].length;
    if (selectAllCb) {
      selectAllCb.checked = n > 0 && n === filteredCount;
      selectAllCb.indeterminate = n > 0 && n < filteredCount;
    }
    setSelectAllToggle(bar.querySelector('[data-bulk="select-all"]'), n > 0 && n === filteredCount);
  }

  function wireMediaBulkBar(type) {
    const selectBtn = document.getElementById(`${type}-select-btn`);
    if (selectBtn) {
      selectBtn.addEventListener("click", () => {
        mediaSelectionMode[type] = !mediaSelectionMode[type];
        if (!mediaSelectionMode[type]) {
          mediaSelection[type].clear();
        }
        updateMediaBulkBar(type);
        renderMediaList(type);
      });
    }

    document.getElementById(`${type}-bulk-bar`).addEventListener("click", async (e) => {
      const btn = e.target.closest("button[data-bulk]");
      if (!btn) return;
      const action = btn.dataset.bulk;
      if (action === "select-all") {
        // Doubles as "Deselect all" once everything filtered is already
        // selected -- see setSelectAllToggle(); also what dropping a
        // selection entirely goes through now, since there's no
        // separate Close button anymore.
        const allSelected = mediaSelection[type].size > 0 && mediaSelection[type].size === mediaFilteredCache[type].length;
        if (allSelected) {
          mediaSelectionMode[type] = false;
          mediaSelection[type].clear();
        } else {
          mediaSelectionMode[type] = true;
          mediaFilteredCache[type].forEach((it) => mediaSelection[type].add(it.id));
        }
        updateMediaBulkBar(type);
        renderMediaList(type);
        return;
      }

      const selected = mediaListCache[type].filter((it) => mediaSelection[type].has(it.id));
      let targets = [];
      let call = null;
      if (action === "queue") {
        targets = selected.filter((it) => it.status === "discovered");
        call = (it) => api(`/api/discovered/${it.discovered_id}/queue`, { method: "POST" });
      } else if (action === "download") {
        // Same eligibility as the per-row Download action -- see
        // mediaRowActions(): Streamable (has .strm left to fetch), not
        // already mid-download.
        targets = selected.filter((it) => it.status === "ready" && (it.streamable_count || 0) > 0);
        call = (it) => api(`/api/media/${mediaIdToUrl(it.id)}/download`, { method: "POST" });
      } else if (action === "cancel") {
        targets = selected.filter((it) => it.status === "queued" || it.status === "running");
        call = (it) => api(`/api/tasks/${it.task_id}/cancel`, { method: "POST" });
      } else if (action === "remove") {
        // Hard removal -- deletes any exported files *and* whatever
        // queue/library record exists, together (see removeMediaItem) --
        // everything except "running" (cancel it first).
        targets = selected.filter((it) => it.status !== "running");
        call = (it) => removeMediaItem(it, type);
      }
      if (!targets.length) { mediaStatus(type, "No selected item is eligible for that action.", true); return; }

      btn.disabled = true;
      try {
        const results = await mapWithConcurrency(targets, call);
        const failed = results.filter((r) => r.status === "rejected").length;
        mediaSelection[type].clear();
        mediaSelectionMode[type] = false;
        updateMediaBulkBar(type);
        await loadMediaList(type);
        mediaStatus(
          type,
          failed ? `${targets.length - failed}/${targets.length} succeeded, ${failed} failed.` : "",
          !!failed
        );
      } finally {
        btn.disabled = false;
      }
    });
  }
  wireMediaBulkBar("movies");
  wireMediaBulkBar("series");

  wireColumnPickerToggle("movies");
  wireColumnPickerToggle("series");
  renderMediaTableHead("movies");
  renderMediaTableHead("series");
  wireMediaViewToggle("movies");
  wireMediaViewToggle("series");

  // -- media detail page ----------------------------------------------------
  const mediaDetailHero = document.getElementById("media-detail-hero");
  const mediaDetailTitle = document.getElementById("media-detail-title");
  const mediaDetailMeta = document.getElementById("media-detail-meta");
  const mediaDetailPathLabel = document.getElementById("media-detail-path-label");
  const mediaDetailPathText = document.getElementById("media-detail-path-text");
  const mediaDetailPathInput = document.getElementById("media-detail-path-input");
  const mediaDetailPathEditBtn = document.getElementById("media-detail-path-edit-btn");
  const mediaDetailPathSaveBtn = document.getElementById("media-detail-path-save-btn");
  const mediaDetailPathCancelBtn = document.getElementById("media-detail-path-cancel-btn");
  const mediaDetailOverview = document.getElementById("media-detail-overview");
  const mediaDetailSeasons = document.getElementById("media-detail-seasons");
  const mediaDetailFiles = document.getElementById("media-detail-files");
  const mediaBackBtn = document.getElementById("media-back-btn");
  const mediaRefreshBtn = document.getElementById("media-refresh-btn");
  const mediaDeleteBtn = document.getElementById("media-delete-btn");
  const mediaDetailStatus = document.getElementById("media-detail-status");
  const mediaDetailDownloadPanel = document.getElementById("media-detail-download");
  const BACKDROP_BASE = "https://image.tmdb.org/t/p/w1280";

  let currentMediaId = null;
  let currentMediaOriginPage = "movies";

  function mediaIdToUrl(id) {
    return id.split("/").map(encodeURIComponent).join("/");
  }

  // Each of these used to take a `btn` and manage its own disabled/error
  // state -- now that every call site goes through mountActionButtons()
  // (which already disables-during-click and reports errors via
  // onError), they just do the request + reload and let errors propagate.
  async function deleteMediaFile(rel_path) {
    await api(`/api/media/${mediaIdToUrl(currentMediaId)}/file?path=${encodeURIComponent(rel_path)}`, { method: "DELETE" });
    await loadMediaDetail(currentMediaId);
  }

  async function downloadMediaFile(rel_path) {
    await api(`/api/media/${mediaIdToUrl(currentMediaId)}/file/download?path=${encodeURIComponent(rel_path)}`, { method: "POST" });
    await loadMediaDetail(currentMediaId);
  }

  async function revertMediaFile(rel_path) {
    await api(`/api/media/${mediaIdToUrl(currentMediaId)}/file/revert?path=${encodeURIComponent(rel_path)}`, { method: "POST" });
    await loadMediaDetail(currentMediaId);
  }

  // One file row's actions cell: Download (still a .strm) or Revert to
  // stream (already downloaded, restores the .strm from the link
  // sidecar core.write_link_sidecar recorded -- see core.
  // revert_link_to_stream) -- either way alongside the existing Remove.
  // Icon-only buttons, same ACTION_ICONS/mountActionButtons convention
  // every other action button in the app already uses. Download is
  // disabled while a download task is already active for this title
  // (downloadTaskActive) so the same file can't be queued twice; Remove
  // stays available either way. `f` is an on-disk file dict
  // (filename/rel_path/downloaded) -- either a movie file or one
  // episode's `on_disk` (see renderSeasonBlock; never called for an
  // episode with no on_disk at all -- nothing to act on).
  function buildFileActionsCell(f, downloadTaskActive) {
    const actionsTd = document.createElement("td");
    actionsTd.className = "q-actions-cell";
    const row = document.createElement("div");
    row.className = "row wrap q-actions";

    const actions = [];
    if (f.downloaded) {
      actions.push({ label: "Revert to stream", cls: "btn-ghost", onClick: () => revertMediaFile(f.rel_path) });
    } else {
      actions.push({
        label: "Download", cls: "btn-primary", disabled: downloadTaskActive,
        title: downloadTaskActive ? "A download is already in progress for this title" : "Download",
        onClick: () => downloadMediaFile(f.rel_path),
      });
    }
    actions.push({ label: "Remove", cls: "btn-danger", onClick: () => deleteMediaFile(f.rel_path) });
    mountActionButtons(row, actions, (err) => setStatus(mediaDetailStatus, err.message, true));

    actionsTd.appendChild(row);
    return actionsTd;
  }

  // Live glance at an in-flight download for this title (detail.
  // download_task, see app.py's get_media_detail_route) -- same
  // byte-level progress buildRowDescriptor's Activity row shows,
  // condensed into this page instead of a second poll loop; Activity
  // stays the authoritative queue view (View log, etc. still live there).
  function renderDownloadTaskPanel(task) {
    mediaDetailDownloadPanel.innerHTML = "";
    if (!task) {
      mediaDetailDownloadPanel.classList.add("hidden");
      return;
    }
    mediaDetailDownloadPanel.classList.remove("hidden");

    const total = task.total_items || 0;
    const done = task.files_written || 0;
    let bytesDone = 0, bytesTotal = 0, bytesTotalKnown = total > 0;
    for (const it of task.items || []) {
      bytesDone += it.bytes_downloaded || 0;
      if (it.bytes_total != null) bytesTotal += it.bytes_total;
      else if (it.status !== "done") bytesTotalKnown = false;
    }

    const label = document.createElement("div");
    label.className = "q-title";
    label.textContent = task.status === "running" ? "Downloading…" : "Download queued…";
    mediaDetailDownloadPanel.appendChild(label);

    const barWrap = document.createElement("div");
    barWrap.className = "progress-bar";
    const fill = document.createElement("div");
    fill.className = "progress-fill";
    if (bytesTotalKnown && bytesTotal > 0) {
      fill.style.width = `${Math.min(100, Math.round((bytesDone / bytesTotal) * 100))}%`;
    } else {
      fill.classList.add("is-indeterminate");
    }
    barWrap.appendChild(fill);
    mediaDetailDownloadPanel.appendChild(barWrap);

    const textParts = [];
    if (bytesTotalKnown && bytesTotal > 0) textParts.push(`${formatBytes(bytesDone)} / ${formatBytes(bytesTotal)}`);
    else if (bytesDone > 0) textParts.push(`${formatBytes(bytesDone)} downloaded so far`);
    textParts.push(`${done}/${total} file(s)`);
    if (task.current_item) textParts.push(task.current_item);
    const text = document.createElement("div");
    text.className = "hint";
    text.textContent = textParts.join(" · ");
    mediaDetailDownloadPanel.appendChild(text);

    const cancelBtn = document.createElement("button");
    cancelBtn.type = "button";
    cancelBtn.className = "btn btn-small btn-danger";
    cancelBtn.textContent = "Cancel";
    cancelBtn.onclick = async () => {
      cancelBtn.disabled = true;
      try {
        await api(`/api/tasks/${task.id}/cancel`, { method: "POST" });
        await loadMediaDetail(currentMediaId);
      } catch (err) {
        setStatus(mediaDetailStatus, err.message, true);
        cancelBtn.disabled = false;
      }
    };
    mediaDetailDownloadPanel.appendChild(cancelBtn);
  }

  // One season's collapsible block -- a native <details>/<summary> (no
  // existing accordion component in the codebase; <details> gives free
  // keyboard/accessibility support with no state machine of its own to
  // write). Defaults open only for the highest season number, closed for
  // the rest; a per-(media id, season) localStorage entry overrides that
  // default once the user's toggled it, so their layout sticks across
  // visits. `season.episodes[i]` is the merged shape app.py's
  // get_media_detail_route always sends now -- {episode_number, name,
  // air_date, on_disk} -- on_disk is null for an episode TMDB knows
  // about but nothing's been fetched for at all ("Missing"), otherwise
  // the same {filename, rel_path, downloaded} buildFileActionsCell()
  // already expects.
  function seasonOpenStorageKey(mediaId, seasonNum) {
    return `febarr-season-open-${mediaId}-${seasonNum}`;
  }

  function renderSeasonBlock(mediaId, season, maxSeasonNum, downloadTaskActive) {
    const block = document.createElement("details");
    block.className = "season-block";
    const storageKey = seasonOpenStorageKey(mediaId, season.season);
    const stored = localStorage.getItem(storageKey);
    block.open = stored != null ? stored === "1" : season.season === maxSeasonNum;
    block.addEventListener("toggle", () => {
      localStorage.setItem(storageKey, block.open ? "1" : "0");
    });

    const summary = document.createElement("summary");
    summary.className = "season-header";
    const caret = document.createElement("span");
    caret.className = "material-symbols-outlined season-caret";
    caret.textContent = "chevron_right";
    summary.appendChild(caret);

    const haveCount = season.episodes.filter((e) => e.on_disk).length;
    const label = document.createElement("span");
    label.className = "season-header-label";
    label.textContent = `${season.label} · ${haveCount}/${season.episodes.length} episode(s)`;
    summary.appendChild(label);

    const hasStreamable = season.episodes.some((e) => e.on_disk && !e.on_disk.downloaded);
    if (hasStreamable) {
      const actionsWrap = document.createElement("div");
      actionsWrap.className = "season-header-actions";
      // Season-level "Download all" -- one task covering every
      // streamable file in this season (see app.py's
      // download_media_route ?season=N), not a client-side loop over
      // the single-file endpoint (which 400s a second call while one
      // download task is already active for the title).
      mountActionButtons(actionsWrap, [{
        label: "Download", cls: "btn-primary", disabled: downloadTaskActive,
        title: downloadTaskActive ? "A download is already in progress for this title" : `Download all of ${season.label}`,
        onClick: async () => {
          await api(`/api/media/${mediaIdToUrl(mediaId)}/download?season=${season.season}`, { method: "POST" });
          await loadMediaDetail(mediaId);
        },
      }], (err) => setStatus(mediaDetailStatus, err.message, true));
      // Stop the click from also toggling the <details> open state --
      // the whole <summary> is the native disclosure's click target.
      actionsWrap.addEventListener("click", (e) => e.stopPropagation());
      summary.appendChild(actionsWrap);
    }
    block.appendChild(summary);

    const table = document.createElement("table");
    table.className = "data-table";
    const tbody = document.createElement("tbody");
    for (const ep of season.episodes) {
      const tr = document.createElement("tr");
      const numTd = document.createElement("td");
      numTd.style.width = "50px";
      numTd.textContent = ep.episode_number != null ? String(ep.episode_number) : "";
      const nameTd = document.createElement("td");
      nameTd.textContent = ep.name || (ep.on_disk ? ep.on_disk.filename : "");
      const dateTd = document.createElement("td");
      dateTd.className = "hint";
      dateTd.textContent = ep.air_date || "";
      const statusTd = document.createElement("td");
      const statusBadge = document.createElement("span");
      let statusKey = "missing", statusText = "Missing";
      if (ep.on_disk) {
        statusKey = ep.on_disk.downloaded ? "ready" : "queued";
        statusText = ep.on_disk.downloaded ? "Downloaded" : "Ready";
      }
      statusBadge.className = `status-badge status-${statusKey}`;
      statusBadge.textContent = statusText;
      statusTd.appendChild(statusBadge);

      tr.appendChild(numTd);
      tr.appendChild(nameTd);
      tr.appendChild(dateTd);
      tr.appendChild(statusTd);
      tr.appendChild(ep.on_disk ? buildFileActionsCell(ep.on_disk, downloadTaskActive) : document.createElement("td"));
      tbody.appendChild(tr);
    }
    table.appendChild(tbody);
    const scroll = document.createElement("div");
    scroll.className = "table-scroll";
    scroll.appendChild(table);
    block.appendChild(scroll);
    return block;
  }

  // Splits a "<Category label>/<folder name>" media id into its two
  // halves -- the label prefix is shown as fixed context (renaming never
  // moves a title between Movies/Series, only a real re-classification
  // would), the folder name is the only part the path-edit control lets
  // you change. See wireMediaDetailPathEdit() below.
  function splitMediaPath(id) {
    const idx = (id || "").indexOf("/");
    if (idx === -1) return ["", id || ""];
    return [id.slice(0, idx), id.slice(idx + 1)];
  }

  // Tracks whether the path row is mid-edit so the background poll
  // (refreshVisibleMediaDetail, every few seconds) doesn't yank the
  // textbox out from under someone still typing into it -- renderMediaDetail
  // runs on every poll tick same as a real navigation, so without this
  // guard the edit UI would get force-closed by exitMediaDetailPathEdit()
  // moments after opening it.
  let mediaDetailPathEditing = false;

  function exitMediaDetailPathEdit() {
    mediaDetailPathEditing = false;
    mediaDetailPathText.classList.remove("hidden");
    mediaDetailPathEditBtn.classList.remove("hidden");
    mediaDetailPathInput.classList.add("hidden");
    mediaDetailPathSaveBtn.classList.add("hidden");
    mediaDetailPathCancelBtn.classList.add("hidden");
  }

  function renderMediaDetail(detail) {
    mediaDetailTitle.textContent = detail.year ? `${detail.title} (${detail.year})` : detail.title;
    const total = detail.file_count || 0;
    const downloadedNow = detail.downloaded_count || 0;
    const downloading = !!detail.download_task;
    const noun = detail.is_series ? "episode(s)" : "file(s)";
    const metaBits = [
      detail.is_series ? "Series" : "Movie",
      `${downloadedNow}/${total} ${noun} ${downloading ? "downloading" : "downloaded"}`,
    ];
    mediaDetailMeta.textContent = metaBits.join(" · ");

    if (!mediaDetailPathEditing) {
      const [pathLabel, pathFolder] = splitMediaPath(detail.id);
      mediaDetailPathLabel.textContent = `${pathLabel}/`;
      mediaDetailPathText.textContent = pathFolder;
      exitMediaDetailPathEdit();
    }

    const meta = detail.metadata;
    if (meta && meta.backdrop_path) {
      mediaDetailHero.style.backgroundImage = `url(${BACKDROP_BASE}${meta.backdrop_path})`;
      mediaDetailHero.classList.remove("no-image");
    } else {
      mediaDetailHero.style.backgroundImage = "";
      mediaDetailHero.classList.add("no-image");
    }
    mediaDetailOverview.textContent = (meta && meta.overview) || "";

    renderDownloadTaskPanel(detail.download_task);
    const downloadTaskActive = !!detail.download_task;

    mediaDetailSeasons.innerHTML = "";
    mediaDetailFiles.innerHTML = "";

    if (detail.is_series) {
      const maxSeasonNum = detail.seasons.length ? Math.max(...detail.seasons.map((s) => s.season)) : null;
      for (const season of detail.seasons) {
        mediaDetailSeasons.appendChild(renderSeasonBlock(detail.id, season, maxSeasonNum, downloadTaskActive));
      }
    } else {
      const table = document.createElement("table");
      table.className = "data-table";
      const tbody = document.createElement("tbody");
      for (const f of detail.files) {
        const tr = document.createElement("tr");
        const nameTd = document.createElement("td");
        nameTd.textContent = f.filename;
        const statusTd = document.createElement("td");
        const statusBadge = document.createElement("span");
        statusBadge.className = `status-badge status-${f.downloaded ? "ready" : "queued"}`;
        statusBadge.textContent = f.downloaded ? "Downloaded" : "Ready";
        statusTd.appendChild(statusBadge);
        tr.appendChild(nameTd);
        tr.appendChild(statusTd);
        tr.appendChild(buildFileActionsCell(f, downloadTaskActive));
        tbody.appendChild(tr);
      }
      table.appendChild(tbody);
      const scroll = document.createElement("div");
      scroll.className = "table-scroll";
      scroll.appendChild(table);
      mediaDetailFiles.appendChild(scroll);
    }
  }

  // opts.silent skips the "Loading..." (and error-text) reset -- used by
  // the background poll (refreshVisibleMediaDetail) so the title doesn't
  // visibly flash back to "Loading..." every few seconds even though the
  // page already has data; a real navigation (showMediaDetail) or a
  // deliberate "Refresh metadata" click still show it. opts.forceRefresh
  // bypasses the server's TMDB episode-grouping cache (see
  // tmdb_episodes.py) -- only "Refresh metadata" sets it.
  async function loadMediaDetail(id, opts) {
    opts = opts || {};
    if (!opts.silent) mediaDetailTitle.textContent = "Loading...";
    try {
      const qs = opts.forceRefresh ? "?refresh=1" : "";
      const detail = await api(`/api/media/${mediaIdToUrl(id)}${qs}`);
      renderMediaDetail(detail);
    } catch (err) {
      if (!opts.silent) {
        mediaDetailTitle.textContent = "Couldn't load this title.";
        mediaDetailMeta.textContent = err.message;
      }
    }
  }

  function showMediaDetail(id, originPage) {
    currentMediaId = id;
    currentMediaOriginPage = originPage || lastListPage;
    showPage("media-detail");
    loadMediaDetail(id);
  }

  // Keeps an open detail page's download progress panel (and per-file
  // Streamable/Downloaded status) live -- same throttled-tick shape as
  // refreshVisibleMediaList(), just a single title's cheap detail fetch
  // instead of a whole-library scan, so a shorter interval is fine.
  const MEDIA_DETAIL_POLL_INTERVAL_MS = 3000;
  const mediaDetailPollState = { lastRefreshAt: 0, inFlight: false };
  function refreshVisibleMediaDetail() {
    if (!document.getElementById("page-media-detail").classList.contains("active") || !currentMediaId) return;
    const now = Date.now();
    if (mediaDetailPollState.inFlight || now - mediaDetailPollState.lastRefreshAt < MEDIA_DETAIL_POLL_INTERVAL_MS) return;
    mediaDetailPollState.inFlight = true;
    mediaDetailPollState.lastRefreshAt = now;
    loadMediaDetail(currentMediaId, { silent: true }).finally(() => { mediaDetailPollState.inFlight = false; });
  }

  // -- editable path (folder name) on the detail page ------------------
  // Renames just the folder-name segment of "<Category label>/<folder
  // name>" -- the category prefix is fixed (shown as static text, not
  // part of the input) since changing it is a re-classification, not a
  // rename. Server (app.py's rename route) handles moving any real files
  // on disk and retargeting whatever task/discovered record was keyed to
  // the old id, so this is just: send the new name, then reload the
  // detail page (and the underlying Movies/Series list cache) under
  // whatever id comes back.
  function enterMediaDetailPathEdit() {
    mediaDetailPathEditing = true;
    mediaDetailPathInput.value = mediaDetailPathText.textContent;
    mediaDetailPathText.classList.add("hidden");
    mediaDetailPathEditBtn.classList.add("hidden");
    mediaDetailPathInput.classList.remove("hidden");
    mediaDetailPathSaveBtn.classList.remove("hidden");
    mediaDetailPathCancelBtn.classList.remove("hidden");
    mediaDetailPathInput.focus();
    mediaDetailPathInput.select();
  }

  async function saveMediaDetailPathEdit() {
    const newName = mediaDetailPathInput.value.trim();
    if (!currentMediaId || !newName || newName === mediaDetailPathText.textContent) {
      exitMediaDetailPathEdit();
      return;
    }
    mediaDetailPathSaveBtn.disabled = true;
    try {
      const result = await api(`/api/media/${mediaIdToUrl(currentMediaId)}/rename`, {
        method: "POST",
        body: JSON.stringify({ name: newName }),
      });
      currentMediaId = result.id;
      exitMediaDetailPathEdit();
      setStatus(mediaDetailStatus, "Renamed.", false);
      await loadMediaDetail(currentMediaId);
      await loadMediaList(currentMediaOriginPage);
    } catch (err) {
      setStatus(mediaDetailStatus, err.message, true);
    } finally {
      mediaDetailPathSaveBtn.disabled = false;
    }
  }

  mediaDetailPathEditBtn.addEventListener("click", enterMediaDetailPathEdit);
  mediaDetailPathCancelBtn.addEventListener("click", exitMediaDetailPathEdit);
  mediaDetailPathSaveBtn.addEventListener("click", saveMediaDetailPathEdit);
  mediaDetailPathInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      saveMediaDetailPathEdit();
    } else if (e.key === "Escape") {
      e.preventDefault();
      exitMediaDetailPathEdit();
    }
  });

  mediaBackBtn.addEventListener("click", () => showPage(currentMediaOriginPage));

  mediaRefreshBtn.addEventListener("click", () => {
    if (currentMediaId) loadMediaDetail(currentMediaId, { forceRefresh: true });
  });

  mediaDeleteBtn.addEventListener("click", async () => {
    if (!currentMediaId) return;
    mediaDeleteBtn.disabled = true;
    try {
      await api(`/api/media/${mediaIdToUrl(currentMediaId)}`, { method: "DELETE" });
      showPage(currentMediaOriginPage);
      await loadMediaList(currentMediaOriginPage);
    } catch (err) {
      setStatus(mediaDetailStatus, err.message, true);
    } finally {
      mediaDeleteBtn.disabled = false;
    }
  });

  showPage(localStorage.getItem("febarr-page") || "activity");
  loadSettings().catch((err) => console.error("Failed to load settings:", err.message));
  poll();
})();
