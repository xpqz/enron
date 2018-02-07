package main

import (
	"fmt"
	"io/ioutil"
	"net/mail"
	"os"
	"path/filepath"
	"strings"

	cdt "github.com/cloudant-labs/go-cloudant"
)

// EnronEmail ...
type EnronEmail struct {
	MessageID string   `json:"message_id"`
	Date      string   `json:"date"`
	From      string   `json:"from"`
	To        []string `json:"to"`
	Cc        []string `json:"cc"`
	Bcc       []string `json:"bcc"`
	Subject   string   `json:"subject"`
	Body      string   `json:"body"`
}

func findEmails(dir string) chan string {

	list := make(chan string, 1000)

	go func() {
		defer close(list)
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}
			if strings.HasSuffix(path, ".") {
				list <- path
			}
			return nil
		})
	}()

	return list
}

func makeClient() (*cdt.CouchClient, error) {
	username := os.Getenv("COUCH_USER")
	password := os.Getenv("COUCH_PASS")
	hostURL := os.Getenv("COUCH_HOST_URL")

	if username == "" || password == "" || hostURL == "" {
		return nil, fmt.Errorf("Expected env vars COUCH_USER, COUCH_PASS and COUCH_HOST_URL to be set")
	}

	return cdt.CreateClient(username, password, hostURL, 4)
}

func makeDatabase() (*cdt.Database, error) {
	client, err := makeClient()
	if err != nil {
		return nil, err
	}

	return client.GetOrCreate("enron")
}

func parseEmail(path string, uploader *cdt.Uploader) {
	msg, err := ioutil.ReadFile(path)
	if err != nil {
		return
	}

	r := strings.NewReader(string(msg))
	m, err := mail.ReadMessage(r)
	if err != nil {
		return
	}

	header := m.Header

	email := EnronEmail{
		MessageID: header.Get("Messasge-ID"),
		Date:      header.Get("Date"),
		From:      header.Get("From"),
		To:        strings.Split(header.Get("To"), ", "),
		Cc:        strings.Split(header.Get("Cc"), ", "),
		Bcc:       strings.Split(header.Get("Bcc"), ", "),
		Subject:   header.Get("Subject"),
	}

	body, err := ioutil.ReadAll(m.Body)
	if err != nil {
		return
	}

	email.Body = string(body)

	uploader.Upload(email)
}

func main() {
	list := findEmails("/Users/stefan/Downloads/maildir")

	enronDB, err := makeDatabase()
	if err != nil {
		fmt.Println(err)
		return
	}

	bulker := enronDB.Bulk(5000, 1024*1024, 0)
	processedEmails := 0
	for f := range list {
		parseEmail(f, bulker)
		processedEmails++
		if processedEmails%1000 == 0 {
			fmt.Println(".")
		}
	}

	bulker.Stop()

	fmt.Printf("Processed %d emails\n", processedEmails)
}
