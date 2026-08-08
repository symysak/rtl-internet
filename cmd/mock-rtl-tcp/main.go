// Command mock-rtl-tcp is a fake rtl_tcp for developing against without a
// dongle, and for reproducing bad-link behaviour on demand.
//
//	mock-rtl-tcp --listen 127.0.0.1:1234 --jitter 80ms --drop-after 30s
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/symysak/rtl-internet/internal/mockrtl"
)

func main() {
	cfg := mockrtl.DefaultConfig()

	listen := flag.String("listen", "127.0.0.1:1234", "待ち受けアドレス")
	sampleRate := flag.Uint("sample-rate", uint(cfg.SampleRate), "生成するサンプルレート")
	flag.Float64Var(&cfg.RateFactor, "rate-factor", cfg.RateFactor,
		"公称レートに対する倍率。0.9 で帯域不足、1.001 で水晶ドリフトを模擬")
	flag.DurationVar(&cfg.Burst, "burst", cfg.Burst, "送出周期。大きいほどバースティ")
	flag.DurationVar(&cfg.Jitter, "jitter", cfg.Jitter, "各バーストに乗せる最大ランダム遅延")
	flag.DurationVar(&cfg.DropAfter, "drop-after", cfg.DropAfter,
		"接続からこの時間後に切断する（再接続の確認用）")
	flag.Parse()

	cfg.SampleRate = uint32(*sampleRate)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv, err := mockrtl.Start(ctx, *listen, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "エラー:", err)
		os.Exit(1)
	}
	fmt.Printf("mock rtl_tcp: %s (%d sps, rate-factor %.3f, jitter %s, drop-after %s)\n",
		srv.Addr(), cfg.SampleRate, cfg.RateFactor, cfg.Jitter, cfg.DropAfter)

	<-ctx.Done()
	time.Sleep(50 * time.Millisecond)
}
