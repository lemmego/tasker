package web

// dashboardHTML is the document: a sidebar, a header and the panes the
// script fills.
//
// No go:embed, no bundler, no npm. The framework has no distribution path
// for frontend assets — the one package.json in the tree is marked private
// and publishes nothing — so a module that wants a UI either ships a string
// or ships nothing. lemmego/observe's dashboard makes the same call, and
// this one deliberately looks like it.
//
// Everything is fetched from relative URLs, so the dashboard works at
// whatever prefix it is mounted under without the prefix being interpolated
// into the markup. That is load-bearing rather than tidy: the server's own
// prefix comes from configuration, and a dashboard that baked it in would
// break for anybody who mounted it somewhere other than the default.
const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>tasker</title>
<style>` + dashboardCSS + `</style>
<script>` + dashboardThemeJS + `</script>
</head>
<body>
<div class="app">
  <nav class="side">
    <div class="brand"><b>tasker</b><span id="status"></span></div>
    <div id="nav"></div>
  </nav>
  <main class="main">
    <div class="head" id="head">
      <h1 id="title">Overview</h1>
      <span class="sub" id="subtitle"></span>
      <span class="spacer"></span>
      <button id="live" class="on" title="Refresh as the queue changes">Live</button>
      <button id="prune" title="Delete finished jobs past their retention">Prune</button>
      <button id="theme" class="icon" title="Switch between light and dark">&#9681;</button>
    </div>
    <div class="bar" id="bar">
      <input id="q" placeholder="search payload or kind&hellip;" style="min-width:230px">
      <select id="queue"><option value="">every queue</option></select>
      <select id="kind"><option value="">every kind</option></select>
      <button id="reload">Reload</button>
      <span class="spacer"></span>
      <span class="sub" id="counts"></span>
    </div>
    <div id="notices"></div>
    <div id="chips"></div>
    <div id="list"></div>
    <div id="detail"></div>
  </main>
</div>
<div class="toast" id="toast"></div>
<script>` + dashboardJS + `</script>
</body>
</html>`
