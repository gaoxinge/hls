# HLS 下载助手

纯 TypeScript Chrome 扩展，发现网页请求中的 HLS 播放列表，在用户确认后下载、解密并保存 TS 或 MP4。最低支持 Chrome 134。安装和使用已构建的扩展不需要 Node.js、Python 或 FFmpeg。

## 安装和使用

### 方式一：安装 Release 包

1. 打开本项目的 [GitHub Releases](https://github.com/gaoxinge/hls/releases)，选择所需版本。
2. 在该版本的 **Assets** 中下载 `hls-browser-downloader-<版本>-chrome.zip`。GitHub 自动提供的 `Source code (zip)` / `Source code (tar.gz)` 是源码，不是可直接加载的扩展。
3. 将 ZIP 解压到一个固定目录，例如 Windows 的 `D:\Extensions\hls`。解压后应能找到 `manifest.json`、`background.js`、`manager.html` 等文件。
4. 打开 Chrome 的 `chrome://extensions`，启用“开发者模式”，点击“加载已解压的扩展程序”，选择**直接包含 `manifest.json` 的文件夹**。不要选择 ZIP 文件。

如果 Releases 中还没有扩展 ZIP，请按下方源码构建步骤安装。加载后请保留解压目录，Chrome 仍需要读取其中的文件；这不是 Chrome 应用商店安装包。

### 方式二：从源码构建

安装 [`.nvmrc`](.nvmrc) 指定版本的 Node.js，以及 Git。在终端执行：

```sh
git clone https://github.com/gaoxinge/hls.git
cd hls
npm ci
npm run build
```

以下开发命令均在**项目根目录，即包含 `package.json` 的目录**运行。`npm ci` 安装依赖，并通过 `postinstall` 生成 WXT 的类型配置；生产构建不需要 FFmpeg。

构建成功后，扩展目录是 **`<项目根目录>/.output/chrome-mv3/`**。例如源码位于 `D:\Projects\hls`，应在 Chrome 中加载 `D:\Projects\hls\.output\chrome-mv3`，该目录下有 `manifest.json`。

`.output/` 是构建生成目录，受 [`.gitignore`](.gitignore) 忽略，因此刚克隆仓库或下载源码时看不到它。它不位于 `src/` 内，也不是 Chrome 的视频下载目录。找不到时请先确认 `npm run build` 成功，再检查项目根目录；部分文件管理器会隐藏以点开头的目录。

### 下载视频

1. 允许扩展访问源网页及视频 CDN，然后在源网页播放视频。安装前已发生的请求需要刷新页面或重新播放才能发现。
2. 点击扩展图标查看候选；也可通过“下载任务”进入管理页，或点击发现通知。
3. 选择最高/最低可用清晰度；“查看清晰度”可加载并手动指定变体。点击“下载”确认，管理页会打开或复用。勾选“另存为”可选择保存位置。
4. 保持管理页打开，直到任务显示“已完成”。关闭弹窗不影响管理页中的下载。
5. 在 Chrome 下载记录 `chrome://downloads` 中找到文件并查看其保存位置。

自动发现不会主动请求媒体分片。“查看清晰度”只读取播放列表；点击“下载”后才请求密钥、初始化段和媒体分片。广告也可能产生候选。自动清晰度策略遇到不兼容变体会尝试其他候选；手动指定失败时会报错。

### 更新扩展

等待当前下载结束，关闭管理页。Release 安装方式使用新 ZIP 更新原解压目录；源码安装方式更新源码后执行 `npm ci` 和 `npm run build`。然后在 `chrome://extensions` 点击该扩展的“重新加载”，重新打开管理页。保留原安装目录和浏览器配置，避免通过卸载重装来更新需要保留历史记录的扩展。

## 支持范围

- 已结束的 TS 点播、主列表与媒体列表、最高/最低自动选择和手动变体。
- 未加密与 identity AES-128：显式或媒体序号 IV、密钥切换、METHOD=NONE。
- 显式与隐式字节范围：要求完整的 206 和匹配的 Content-Range。
- DEFINE 的 NAME/VALUE、IMPORT、QUERYPARAM 变量及相对 URI。
- MAP 初始化段与受约束的连续 fMP4；容器检查通过后输出 `.mp4`。真实媒体样例覆盖 H.264/AAC，不代表所有编码均已验证。
- 加密 MAP 要求显式 IV；SESSION-KEY 不替代媒体 EXT-X-KEY。

暂不支持独立音视频/字幕合流、直播/LL-HLS、初始化变化、不连续时间线、GAP、I-frame-only、动态 Content Steering、SAMPLE-AES 或 DRM，也不提供通用重新封装、TS 转 MP4 或转码。

**引用独立音频列表的主列表会被拒绝**，例如 `EXT-X-STREAM-INF` 的 `AUDIO` 引用了带独立 `URI` 的 `EXT-X-MEDIA`。只下载其中的视频子列表可能得到无声视频；分别下载音频和视频不会自动合流。

签名过期、Cookie、Referer 或站点认证限制可能造成失败。遇到 401/403，刷新源网页重新发现候选。扩展使用 HTTP(S) 主机权限观察网页请求和访问 CDN，不上传浏览记录，也不持久化密钥原文。

## 恢复与存储

最终视频保存位置由 Chrome 下载设置或“另存为”选择决定，与源码目录和 `.output/` 无关。默认建议名称为 `HLS-<UTC 时间戳>.ts` 或 `.mp4`，例如 `HLS-2026-09-12T12-00-21-756Z.mp4`。重名时 Chrome 会调整名称，以下载记录中的实际文件名为准。

分片与合并中的视频临时存放在浏览器的 OPFS，任务和产物描述存放在 IndexedDB，均属于当前扩展的浏览器存储，无需手动定位或复制这些内部文件。

- 管理页或 Chrome 关闭后，重新打开管理页，点击“恢复 / 重试”。恢复使用固定下载计划，校验缓存大小、SHA-256 和可用的源站 ETag/Last-Modified；无源站校验器时不能保证远端内容没有改变。
- 最终文件已经生成但保存失败时，点击“重新保存”可校验并导出缓存。当前导出会显式设置 TS/MP4 的 MIME；早期版本已保存到磁盘的 `.txt` 文件不会被自动修改。
- 成功保存后清理媒体缓存；失败任务可重试或“清理任务”。清理任务不会删除已经导出到下载目录的视频。非旧版历史的非活动任务超过 7 天，会在管理页启动时清理。
- 合并期间通常同时保留分片和完整视频，浏览器存储至少应预留约两倍视频大小，最终下载文件还需要磁盘空间。每个媒体资源上限 32 MiB，初始化资源上限 2 MiB。

从 v1 升级时沿用 `hls-v1` 数据库并升级至版本 2，保留历史；旧活动任务不会自动继续，用户重试后按 v2 规则重新下载。数据库升级后不支持直接回退到 v1。

## 开发与验证

技术栈为 WXT、TypeScript strict、原生 DOM/CSS、Web Crypto、IndexedDB 和 OPFS；播放列表解析使用 `m3u8-parser`，容器检查封装 MP4Box.js 和 mux.js。

完成 `npm ci` 后，可运行基础检查及构建：

```sh
npm run typecheck
npm run lint
npm test
npm run build
```

浏览器测试另需 FFmpeg（包含 `libx264` 编码器）、ffprobe 和 Playwright Chromium。将 FFmpeg/ffprobe 加入 PATH，或用环境变量 `FFMPEG`、`FFPROBE` 指定可执行文件路径，然后执行：

```sh
npx playwright install chromium
npm run test:e2e
```

Linux 还需要浏览器系统依赖，可将安装命令改为 `npx playwright install --with-deps chromium`。浏览器测试加载的是 `.output/chrome-mv3/`，所以修改扩展代码后应先重新 `npm run build`。`test:e2e` 不负责构建扩展。

可选的大文件验证：

```sh
npm run test:large
```

该命令生成 128 秒合成媒体，测试超过 1 GiB 的 TS 和 fMP4 下载、哈希与解码，建议预留至少 6 GiB 可用空间。测试用临时浏览器配置和下载文件在结束时清理；生成的媒体和测试记录保留在 `test-results/`，可以手动删除。FFmpeg 只用于开发测试，不进入扩展运行时。

固定样例的位置与重建方法见 [媒体样例说明](tests/fixtures/media/README.md)。当前 CI 运行基础检查和普通浏览器测试，大文件验证单独运行。

## 项目目录与生成文件

| 路径（相对项目根目录） | 用途与来源 |
| --- | --- |
| `entrypoints/`、`src/` | 扩展入口和业务源码，随仓库提供 |
| `public/` | 图标和第三方许可声明，构建时复制进扩展 |
| `tests/unit/`、`tests/e2e/` | 单元测试和浏览器测试脚本，随仓库提供 |
| `tests/fixtures/media/` | 固定的真实媒体样例，随仓库提供 |
| `node_modules/` | `npm ci` 安装的依赖，Git 忽略 |
| `.wxt/` | WXT 生成的类型与配置；`npm ci` 的 postinstall 会生成，也可运行 `npx wxt prepare` 重建，Git 忽略 |
| `.output/chrome-mv3/` | `npm run build` 或 `npm run zip` 生成的可加载扩展，Git 忽略 |
| `.output/*-chrome.zip` | `npm run zip` 生成的扩展分发包，Git 忽略 |
| `test-results/` | 浏览器测试生成的媒体、JSON 记录和截图，Git 忽略 |
| `coverage/`、`playwright-report/` | 为可选覆盖率或报告工具预留的忽略路径，当前默认命令不保证生成 |

`.gitignore` 仅控制文件是否进入 Git；生成目录可以在本地存在，并可由 Actions 构建后上传为产物或 Release 附件。不要为了让用户获得安装包而把 `.output/` 提交进源码仓库。

开发时可运行 `npm run dev` 启动 WXT 开发模式，使用终端输出的开发扩展路径；开发产物不用于分发。生产分发使用 `npm run zip`。设计背景见 [v2 需求](docs/v2/prd.md)和 [v2 开发方案](docs/v2/dev.md)，当前用户可用能力以本文支持范围及实际代码为准。第三方声明见 [THIRD_PARTY_NOTICES.txt](public/THIRD_PARTY_NOTICES.txt)。

## Tag 自动发布

[GitHub Actions 工作流](.github/workflows/check.yml)对普通分支推送和 PR 执行检查，成功后保存名为 `hls-chrome-extension` 的 Actions Artifact。该产物可以在对应的 Actions 运行页面下载，下载的外层归档中包含扩展 ZIP，安装前需继续解压扩展 ZIP，定位 `manifest.json`。

上传步骤显式允许读取隐藏的 `.output/` 目录，并将范围限制为 `.output/*-chrome.zip`；`.gitignore` 不影响 Actions 上传，但上传 Action 自身默认忽略隐藏文件。

对 tag 推送，检查成功后还会将**同一次构建**的 Chrome ZIP 上传到对应的 GitHub Release，生成发布说明。ZIP 缺失或损坏会令发布任务失败。已有 Release 的重跑会更新同名 ZIP。

发布步骤：

1. 更新 `package.json` 与 `package-lock.json` 的版本并提交改动，确保 tag 指向的提交包含工作流及全部需要发布的源码。
2. 创建并推送对应 tag。例如包版本为 `2.0.1` 且尚未发布该 tag 时：

```sh
git tag v2.0.1
git push origin v2.0.1
```

3. 在 Actions 等待 `verify` 和 `Publish Chrome extension` 成功，再到 Releases 下载附件。

支持任意 tag 名称，建议使用 `v版本号`；工作流不会从 tag 改写包版本，也不强制两者一致。扩展版本和 ZIP 名称取自 `package.json`，例如 `hls-browser-downloader-2.0.1-chrome.zip`。仅在本地创建 tag 不会触发发布。

流程使用 GitHub 提供的 `GITHUB_TOKEN`，构建任务只读，仅发布任务申请 `contents: write`，无需额外配置个人令牌。工作流和发布权限需在仓库中可用；失败原因见 Actions 日志，修复后可重跑。此流程发布 GitHub Release，不会自动上传 Chrome 应用商店。
