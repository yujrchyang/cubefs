package multi_email

import (
	"crypto/tls"
	"fmt"
	"github.com/go-gomail/gomail"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	gridTableCSS = `<style type="text/css">
			table.gridtable {
				font-family: verdana,arial,sans-serif;
				font-size:11px;
				color:#333333;
				border-width: 1px;
				border-color: #666666;
				border-collapse: collapse;
			}
			table.gridtable th {
				border-width: 1px;
				padding: 8px;
				border-style: solid;
				border-color: #666666;
				background-color: #dedede;
			}
			table.gridtable td {
				border-width: 1px;
				padding: 8px;
				border-style: solid;
				border-color: #666666;
				background-color: #ffffff;
			}
			</style>`
	maxEmailElements  = 100
	maxAttachElements = 100000
)

type multiMail struct {
	smtpHost string
	smtpPort int
	user     string
	pwd      string
	from     string
	to       []string
}

var gMultiAlarm *multiMail

func InitMultiMail(smtpPort int, smtpHost, from, user, pwd string, to []string) {
	gMultiAlarm = &multiMail{
		smtpHost: smtpHost,
		smtpPort: smtpPort,
		from:     from,
		to:       to,
		user:     user,
		pwd:      pwd,
	}
	return
}

func Email(subject string, summary string, items [][]string) (err error) {
	if gMultiAlarm.smtpPort == 0 || gMultiAlarm.smtpHost == "" || len(items) < 1 || gMultiAlarm.from == "" {
		return
	}

	email := gomail.NewMessage()
	email.SetHeader("From", gMultiAlarm.from)
	email.SetHeader("To", gMultiAlarm.to...)
	email.SetHeader("Subject", subject)
	email.SetBody("text/html", genTable(summary, items[0], items[1:]))

	// do not attach too many datas
	if len(items) > maxAttachElements {
		items = items[:maxAttachElements]
	}

	// gen attach file
	if len(items) > maxEmailElements {
		pwd, _ := os.Getwd()
		fName := fmt.Sprintf("data_%v.csv", time.Now().Format("200601021504"))
		fPath := path.Join(pwd, fName)
		var fd *os.File
		fd, err = os.OpenFile(fPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			return
		}
		defer func() {
			fd.Close()
			os.Remove(fPath)
		}()
		for _, item := range items {
			fd.WriteString(strings.Join(item, ",") + "\n")
		}
		email.Attach(fPath, gomail.Rename(fName))
	}

	d := gomail.NewDialer(gMultiAlarm.smtpHost, gMultiAlarm.smtpPort, gMultiAlarm.user, gMultiAlarm.pwd)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	if err = d.DialAndSend(email); err != nil {
		return
	}
	return
}

func genTable(summary string, tableHeader []string, tableElements [][]string) string {
	var elements [][]string
	if len(tableElements) > maxEmailElements {
		elements = tableElements[:maxEmailElements]
	} else {
		elements = tableElements
	}

	// css
	html := gridTableCSS
	html += `<table class="gridtable">`

	// header
	html += `<tr><td  colspan="` + strconv.Itoa(len(tableHeader)) + `">` + summary + "</td></tr>"
	html += "<tr>"
	for _, h := range tableHeader {
		html += "<th>" + h + "</th>"
	}
	html += "</tr>"

	// table
	for _, line := range elements {
		html += "<tr>"
		for _, grid := range line {
			html += "<td>" + grid + "</td>"
		}
		html += "</tr>"
	}

	// foot
	if len(tableElements) > maxEmailElements {
		foot := fmt.Sprintf("totally %v items, over max display num(%v) refer to attachments for more", len(tableElements), maxEmailElements)
		html += `<tr><td  colspan="` + strconv.Itoa(len(tableHeader)) + `">` + foot + "</td></tr>"
	}
	html += "</table>"
	return html
}
