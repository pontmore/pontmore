(() => {
  const formatTimes = (root = document) => {
    root.querySelectorAll("time[data-epoch]").forEach((element) => {
      const epoch = Number(element.dataset.epoch);
      if (!Number.isFinite(epoch)) return;
      const date = new Date(epoch * 1000);
      element.dateTime = date.toISOString();
      element.textContent = new Intl.DateTimeFormat(undefined, {
        dateStyle: "medium",
        timeStyle: "short",
      }).format(date);
      element.title = date.toISOString();
    });
  };

  formatTimes();
  const dashboard = document.querySelector("#dashboard");
  const status = document.querySelector("#connection-status");
  if (!status) return;
  if (!dashboard) {
    status.textContent = "Connected";
    return;
  }

  const stream = new EventSource("/ui/stream");
  stream.addEventListener("dashboard", (event) => {
    dashboard.innerHTML = event.data;
    formatTimes(dashboard);
    status.textContent = "Live";
    status.classList.remove("disconnected");
  });
  stream.addEventListener("error", () => {
    status.textContent = "Reconnecting";
    status.classList.add("disconnected");
  });
})();
