// Command rtl-internet is a local rtl_tcp-compatible proxy that makes a remote
// rtl_tcp usable across the Internet.
//
// Point it at a remote rtl_tcp and point SDR++, GQRX or SDR# at its local
// listener. It absorbs jitter, hides reconnections and replays the tuning
// state, none of which the SDR application needs to know about.
//
// The remote rtl_tcp is unauthenticated and unencrypted. Do not expose it
// directly; reach it through a tunnel, e.g.
//
//	ssh -N -L 15678:127.0.0.1:1234 pi@remote
//	rtl-internet --upstream 127.0.0.1:15678
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/symysak/rtl-internet/internal/server"
)

func main() {
	cfg := server.DefaultConfig()

	var (
		logLevel   = flag.String("log-level", "info", "ログレベル (debug|info|warn|error)")
		logFormat  = flag.String("log-format", "text", "ログ形式 (text|json)")
		rcvBufMB   = flag.Float64("rcvbuf", 4, "上流ソケットの受信バッファ (MB)")
		sampleRate = flag.Uint("assume-sample-rate", uint(cfg.AssumeSampleRate),
			"SET_SAMPLE_RATE を受け取るまで仮定するサンプルレート")
	)

	flag.StringVar(&cfg.Listen, "listen", cfg.Listen,
		"下流 SDR ソフト向けの待ち受けアドレス。このプロキシ自身も無認証なので localhost 以外は非推奨")
	flag.StringVar(&cfg.Upstream.Addr, "upstream", "", "リモート rtl_tcp のアドレス (host:port) [必須]")
	flag.DurationVar(&cfg.Pacer.Target, "buffer", cfg.Pacer.Target,
		"ジッタバッファの目標深さ。そのまま追加遷移遅延になる")
	flag.DurationVar(&cfg.BufferMax, "buffer-max", cfg.BufferMax,
		"バッファ上限。超過分は最古から破棄して遅延の青天井を防ぐ")
	flag.DurationVar(&cfg.Pacer.RetunePrebuffer, "retune-prebuffer", cfg.Pacer.RetunePrebuffer,
		"周波数変更でバッファを捨てた後の再充填深さ")
	flag.IntVar(&cfg.CmdRate, "cmd-rate", cfg.CmdRate,
		"同一コマンドの最大送出レート (回/秒)。スライダ操作の連打を間引く")
	flag.DurationVar(&cfg.StatsInterval, "stats-interval", cfg.StatsInterval, "統計ログの出力間隔")
	flag.DurationVar(&cfg.Upstream.IdleTimeout, "idle-timeout", cfg.Upstream.IdleTimeout,
		"上流が無音のまま許容する時間。超えたら切って再接続する")
	flag.DurationVar(&cfg.HeaderTimeout, "header-timeout", cfg.HeaderTimeout,
		"下流接続時にドングル情報を待つ上限")
	flag.BoolVar(&cfg.Takeover, "takeover", cfg.Takeover, "新しい下流接続で既存セッションを置き換える")
	flag.BoolVar(&cfg.KeepUpstream, "keep-upstream", cfg.KeepUpstream,
		"下流が居なくても上流接続を維持する（帯域とドングルを占有し続ける）")

	flag.Parse()

	if cfg.Upstream.Addr == "" {
		fmt.Fprintln(os.Stderr, "エラー: --upstream は必須です")
		flag.Usage()
		os.Exit(2)
	}
	cfg.Upstream.RecvBuf = int(*rcvBufMB * (1 << 20))
	cfg.AssumeSampleRate = uint32(*sampleRate)

	log, err := newLogger(*logLevel, *logFormat)
	if err != nil {
		fmt.Fprintln(os.Stderr, "エラー:", err)
		os.Exit(2)
	}

	if err := validate(cfg); err != nil {
		log.Error("設定が不正です", "err", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := server.New(cfg, log).Run(ctx); err != nil {
		log.Error("終了しました", "err", err)
		os.Exit(1)
	}
}

func validate(cfg server.Config) error {
	if cfg.Pacer.Target <= 0 {
		return fmt.Errorf("--buffer は正の値である必要があります")
	}
	if cfg.BufferMax < cfg.Pacer.Target {
		return fmt.Errorf("--buffer-max (%s) は --buffer (%s) 以上である必要があります",
			cfg.BufferMax, cfg.Pacer.Target)
	}
	if cfg.Pacer.RetunePrebuffer > cfg.Pacer.Target {
		return fmt.Errorf("--retune-prebuffer (%s) は --buffer (%s) 以下である必要があります",
			cfg.Pacer.RetunePrebuffer, cfg.Pacer.Target)
	}
	if cfg.AssumeSampleRate == 0 {
		return fmt.Errorf("--assume-sample-rate は正の値である必要があります")
	}
	return nil
}

func newLogger(level, format string) (*slog.Logger, error) {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("不正なログレベル %q", level)
	}
	opts := &slog.HandlerOptions{Level: lv}

	var h slog.Handler
	switch format {
	case "json":
		h = slog.NewJSONHandler(os.Stderr, opts)
	case "text":
		h = slog.NewTextHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("不正なログ形式 %q", format)
	}
	return slog.New(h), nil
}

func init() {
	origUsage := flag.Usage
	flag.Usage = func() {
		origUsage()
		fmt.Fprintf(flag.CommandLine.Output(),
			"\n例:\n  ssh -N -L 15678:127.0.0.1:1234 pi@remote &\n  rtl-internet --upstream 127.0.0.1:15678\n")
	}
}
