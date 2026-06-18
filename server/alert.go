package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type DingTalkNotifier struct {
	webhookURL string
	client     *http.Client
}

func NewDingTalkNotifier(webhookURL string) *DingTalkNotifier {
	return &DingTalkNotifier{
		webhookURL: webhookURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

type dingTalkMarkdown struct {
	Title string `json:"title"`
	Text  string `json:"text"`
}

type dingTalkMsg struct {
	Msgtype  string           `json:"msgtype"`
	Markdown dingTalkMarkdown `json:"markdown"`
}

func (d *DingTalkNotifier) SendAnomalyAlert(ctx context.Context, result *AnomalyResult) error {
	if d.webhookURL == "" {
		return nil
	}

	emoji := "⚠️"
	levelText := "异常"
	switch result.Severity {
	case "critical":
		emoji = "🔴"
		levelText = "严重"
	case "high":
		emoji = "🟠"
		levelText = "高危"
	case "warning":
		emoji = "🟡"
		levelText = "警告"
	}

	title := fmt.Sprintf("%s 慢查询异常 %s - %s", emoji, levelText, result.TableName)

	text := fmt.Sprintf(`### %s
---
- **表名**: %s
- **当前分钟慢查询数**: %d
- **历史均值**: %.2f 次/分钟
- **标准差**: %.2f
- **3σ阈值**: %.2f
- **异常程度**: %s
- **检测时间**: %s
`,
		title,
		result.TableName,
		result.CurrentCount,
		result.MeanPerMinute,
		result.StdPerMinute,
		result.Threshold,
		levelText,
		time.Now().Format("2006-01-02 15:04:05"),
	)

	msg := dingTalkMsg{
		Msgtype: "markdown",
		Markdown: dingTalkMarkdown{
			Title: title,
			Text:  text,
		},
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal dingtalk msg: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("send dingtalk alert: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("dingtalk api status: %d", resp.StatusCode)
	}

	return nil
}
