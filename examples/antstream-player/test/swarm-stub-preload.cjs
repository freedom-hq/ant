// The page side of the harness's stand-in for Freedom's `window.swarm`
// (test/freedom-harness.cjs): `request({ method, params })` and the
// convenience wrappers the player could call, installed in the page's main
// world before its scripts run, rejecting with Freedom's error shape
// (`code`, `message`, `data.reason`).
const { contextBridge, ipcRenderer, webFrame } = require('electron');

contextBridge.exposeInMainWorld('__swarmStub', {
  request: (method, params) => ipcRenderer.invoke('swarm', method, params),
});

webFrame.executeJavaScript(`(() => {
  const request = async ({ method, params } = {}) => {
    const r = await window.__swarmStub.request(method, params || {});
    if (r.error) throw Object.assign(new Error(r.error.message), { code: r.error.code, data: r.error.data });
    return r.result;
  };
  window.swarm = {
    isFreedomBrowser: true,
    request,
    readFeedEntry: (params) => request({ method: 'swarm_readFeedEntry', params }),
    readSingleOwnerChunk: (params) => request({ method: 'swarm_readSingleOwnerChunk', params }),
  };
})()`);
