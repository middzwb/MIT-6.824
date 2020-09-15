package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

var visited map[string]bool = make(map[string]bool)
var waitfor sync.WaitGroup
var mutex sync.Mutex

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	if depth <= 0 {
		return
	}
	mutex.Lock()
	if v, ok := visited[url]; ok && v {
		mutex.Unlock()
		return
	}

	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		mutex.Unlock()
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	visited[url] = true
	mutex.Unlock()
	for _, u := range urls {
		waitfor.Add(1)
		if true {
			go func(tmp_u string) {
				Crawl(tmp_u, depth-1, fetcher)
				waitfor.Done()
			}(u)
		} else {
			Crawl(u, depth-1, fetcher)
			waitfor.Done()
		}

	}
	return
}

func main() {
	Crawl("https://golang.org/", 4, fetcher)
	waitfor.Wait()
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/pkg/",
			"https://golang.org/pkg/",
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
