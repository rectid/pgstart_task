package xmlparser

import (
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"pgstart_task/database"
)

// AnalyzeXMLDumps analyzeXMLDumps анализирует XML-дампы параллельно
func AnalyzeXMLDumps(dumpDir string) map[string]*database.TableInfo {
	var files []string
	filepath.Walk(dumpDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".xml") {
			return nil
		}
		files = append(files, path)
		return nil
	})

	dumps := make(map[string]*database.TableInfo, len(files))
	var mu sync.Mutex

	jobs := make(chan string)
	var wg sync.WaitGroup
	workerCount := runtime.NumCPU() * 2

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				name := strings.TrimSuffix(filepath.Base(path), ".xml")
				ti := &database.TableInfo{Name: name, Columns: map[string]string{}}

				f, err := os.Open(path)
				if err != nil {
					fmt.Printf("Error opening %s: %v\n", path, err)
					continue
				}
				dec := xml.NewDecoder(f)
				for {
					tok, err := dec.Token()
					if err != nil {
						break
					}
					if se, ok := tok.(xml.StartElement); ok && se.Name.Local == "row" {
						var row struct {
							Attrs []xml.Attr `xml:",any,attr"`
						}
						if dec.DecodeElement(&row, &se) == nil {
							for _, a := range row.Attrs {
								ti.Columns[a.Name.Local] = a.Value
							}
						}
					}
				}
				f.Close()

				mu.Lock()
				dumps[name] = ti
				mu.Unlock()

				fmt.Printf("Parsed %s: %d cols\n", name, len(ti.Columns))
			}
		}()
	}

	for _, path := range files {
		jobs <- path
	}
	close(jobs)
	wg.Wait()

	return dumps
}
