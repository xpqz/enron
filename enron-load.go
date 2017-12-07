package main

import (
	"fmt"
	"io/ioutil"
	"net/mail"
	"os"
	"path/filepath"
	"strings"
	"sync"

	cdt "github.ibm.com/cloudant/go-cloudant"
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

	if username == "" || password == "" {
		return nil, fmt.Errorf("Expected env vars COUCH_USER and COUCH_PASS to be set")
	}

	return cdt.CreateClient(username, password, "https://"+username+".cloudant.com", 5)
}

func makeDatabase() (*cdt.Database, error) {
	client, err := makeClient()
	if err != nil {
		return nil, err
	}

	return client.GetOrCreate("enron")
}

func parseEmail(path string, uploader *cdt.Uploader, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

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

	bulker := enronDB.Bulk(100, 0)
	var wg sync.WaitGroup
	processedEmails := 0
	for f := range list {
		go parseEmail(f, bulker, &wg)
		processedEmails++
		if processedEmails%1000 == 0 {
			fmt.Println(".")
		}
	}

	wg.Wait()

	bulker.Stop()
}
