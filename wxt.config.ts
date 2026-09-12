import { defineConfig } from "wxt";
export default defineConfig({
  manifest: {
    name: "HLS 下载助手",
    description: "发现网页中的 HLS 视频，经确认后下载到本地。",
    minimum_chrome_version: "134",
    permissions: ["webRequest", "storage", "downloads", "notifications"],
    host_permissions: ["http://*/*", "https://*/*"],
    icons: { 16: "icon/16.png", 48: "icon/48.png", 128: "icon/128.png" },
  },
});
