# 真实媒体样例

本目录中的 `ts/` 和 `fmp4/` 是随源码提供的固定测试样例，单元测试及部分浏览器测试会直接读取它们。它们不位于被 Git 忽略的 `test-results/` 中，运行 `npm ci` 也不会重新生成它们。

内容为合成测试图案和 1 kHz 正弦音，由 FFmpeg 4.2.7 生成：160×90、25 fps、4 秒、H.264/AAC。TS 包含 PAT/PMT 与时间戳；fMP4 包含单个初始化段、连续解码时间和相对 moof 的数据偏移。异常样例在测试中从这些文件修改得到。

## 重建固定样例

正常运行测试无需重建。若要更新样例，需要支持 `libx264` 的 FFmpeg；在本目录 `tests/fixtures/media/` 下运行以下命令，输出会覆盖对应样例。目录 `ts/`、`fmp4/` 需已存在。不同 FFmpeg 版本可能产生不同字节，重建后应检查文件变更并运行相关测试。

```sh
ffmpeg -y -f lavfi -i testsrc=size=160x90:rate=25 -f lavfi -i sine=frequency=1000:sample_rate=48000 -t 4 -c:v libx264 -pix_fmt yuv420p -preset ultrafast -g 25 -sc_threshold 0 -c:a aac -b:a 64k -f hls -hls_time 1 -hls_playlist_type vod -hls_segment_filename ts/segment%d.ts ts/list.m3u8
ffmpeg -y -f lavfi -i testsrc=size=160x90:rate=25 -f lavfi -i sine=frequency=1000:sample_rate=48000 -t 4 -c:v libx264 -pix_fmt yuv420p -preset ultrafast -g 25 -sc_threshold 0 -c:a aac -b:a 64k -f hls -hls_time 1 -hls_playlist_type vod -hls_segment_type fmp4 -hls_fmp4_init_filename init.mp4 -hls_segment_filename fmp4/segment%d.m4s fmp4/list.m3u8
```

以上为终端中的 FFmpeg 命令，直接使用 PATH 中的 `ffmpeg`；项目测试脚本则支持通过 `FFMPEG`、`FFPROBE` 环境变量指定工具路径。

## 浏览器测试生成的媒体

[generate.mjs](../../e2e/generate.mjs) 在项目根目录运行，由 `npm run test:e2e` 或 `npm run test:large` 自动调用：普通测试生成 12 秒媒体，大文件测试生成 128 秒媒体，输出到 `<项目根目录>/test-results/media/ts/` 和 `fmp4/`。

`test-results/` 被 Git 忽略，首次运行测试前可能不存在；其内容可以删除并通过测试脚本重新生成，不应替代本目录的固定样例。大文件测试在执行时为 TS 添加合法空包、为 fMP4 添加 `free` box，使实际导出超过 1 GiB，并验证输出哈希及解码结果。

安装测试依赖和运行命令见 [项目 README](../../../README.md#开发与验证)。媒体样例和 FFmpeg 均不进入扩展安装包。
