package main

import (
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pquerna/ffjson/ffjson"
	"github.com/valyala/fasthttp"
)

type Product struct {
	ID string `json:"id"`
}

type ProductsResponse struct {
	Data []Product `json:"data"`
}

type ProductDetails struct {
	XMLName  xml.Name `xml:"potravina"`
	GUID     string   `xml:"guid_potravina,attr"` // Добавляем поле для извлечения guid_potravina
	Name     string   `xml:"nazev"`
	Category string   `xml:"kategorie"`
	Ean      []string `xml:"eans>ean"`
	Energy   string   `xml:"hodnoty>energie"`
	Protein  string   `xml:"hodnoty>bilkoviny"`
	Fat      string   `xml:"hodnoty>tuky"`
	Carb     string   `xml:"hodnoty>sacharidy"`
	Salt     string   `xml:"hodnoty>sul"`
	Water    string   `xml:"hodnoty>voda"`
	Sugar    string   `xml:"hodnoty>cukry"`
	Calcium  string   `xml:"hodnoty>vapnik"`
	GI       string   `xml:"hodnoty>gi"`
	PHE      string   `xml:"hodnoty>phe"`
	Alcohol  string   `xml:"hodnoty>alcohol"`
	Image    string   `xml:"foto"`
	URL      string   `xml:"url"` // Добавляем поле для проверки блокировки
}

type Label struct {
	Name        string `xml:"nazev,attr"`
	Description string `xml:",chardata"`
}

type Cache struct {
	mu    sync.RWMutex
	cache map[string]ProductDetails
}

func (c *Cache) Get(key string) (ProductDetails, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	val, ok := c.cache[key]
	return val, ok
}

func (c *Cache) Set(key string, value ProductDetails) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache[key] = value
}

func main() {
	dataDir := "data-4"
	files, err := os.ReadDir(dataDir)
	if err != nil {
		log.Fatalf("Error reading data directory: %v", err)
	}

	const recordsPerFile = 1000
	fileIndex := 1
	recordCount := 0

	// Функция для создания нового CSV файла
	createCSVFile := func(fileIndex int) (*csv.Writer, *os.File) {
		fileName := fmt.Sprintf("results-4/product_details_%d.csv", fileIndex)
		csvFile, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("Failed to create CSV file: %v", err)
		}
		writer := csv.NewWriter(csvFile)

		// Пишем заголовки в CSV
		headers := []string{
			"ID",
			"Name",
			"Category",
			"EANs",
			"Calories",
			"Proteins",
			"Fats",
			"Carbohydrates",
			"Salt",
			"Water",
			"Sugar",
			"Calcium",
			"GI",
			"PHE",
			"Alcohol",
			"Image",
		}
		if err := writer.Write(headers); err != nil {
			log.Fatalf("Failed to write headers to CSV: %v", err)
		}

		return writer, csvFile
	}

	writer, csvFile := createCSVFile(fileIndex)
	defer csvFile.Close()

	// Кэш для результатов
	cache := &Cache{cache: make(map[string]ProductDetails)}

	// Обрабатываем каждый файл из папки data
	for _, fileInfo := range files {
		if fileInfo.IsDir() {
			continue
		}

		filePath := filepath.Join(dataDir, fileInfo.Name())
		file, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("Error reading JSON file %s: %v", filePath, err)
			continue
		}

		// Парсим JSON
		var productsData ProductsResponse
		if err := ffjson.Unmarshal(file, &productsData); err != nil {
			log.Printf("Error parsing JSON from file %s: %v", filePath, err)
			continue
		}

		products := productsData.Data

		// Канал для управления потоком данных
		productChan := make(chan Product, len(products))
		resultChan := make(chan ProductDetails, len(products))

		// Запускаем горутины для обработки продуктов
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ { // Устанавливаем количество горутин
			wg.Add(1)
			go func() {
				defer wg.Done()
				for product := range productChan {
					if details, ok := cache.Get(product.ID); ok {
						resultChan <- details
						continue
					}

					url := fmt.Sprintf("https://www.dine4fit.com/getPotravina.php?GUID_Potravina=%s&lang=ru&pid=iOShVbzP", product.ID)
					var details ProductDetails
					var err error
					for attempt := 0; attempt < 3; attempt++ { // Максимум 3 попытки
						req := fasthttp.AcquireRequest()
						resp := fasthttp.AcquireResponse()
						defer fasthttp.ReleaseRequest(req)
						defer fasthttp.ReleaseResponse(resp)

						req.SetRequestURI(url)
						if err = fasthttp.Do(req, resp); err != nil {
							log.Printf("Failed to fetch details for ID %s (attempt %d): %v", product.ID, attempt+1, err)
							time.Sleep(time.Second * time.Duration(attempt+1)) // Задержка перед повторной попыткой
							continue
						}

						if err = xml.Unmarshal(resp.Body(), &details); err != nil {
							log.Printf("Failed to decode XML for ID %s (attempt %d): %v", product.ID, attempt+1, err)
							time.Sleep(time.Second * time.Duration(attempt+1)) // Задержка перед повторной попыткой
							continue
						}

						// Проверяем, заблокирован ли продукт
						if details.URL == "" {
							log.Printf("Skipping product ID %s with empty URL", product.ID)
							break
						}

						cache.Set(product.ID, details)
						resultChan <- details
						break
					}
					if err != nil {
						log.Printf("Failed to fetch details for ID %s after 3 attempts", product.ID)
					}
				}
			}()
		}

		// Отправляем продукты в канал
		go func() {
			for _, product := range products {
				productChan <- product
			}
			close(productChan)
		}()

		// Ожидаем завершения всех горутин
		go func() {
			wg.Wait()
			close(resultChan)
		}()

		// Обрабатываем результаты
		for details := range resultChan {
			if recordCount >= recordsPerFile {
				writer.Flush()
				csvFile.Close()
				fileIndex++
				writer, csvFile = createCSVFile(fileIndex)
				defer csvFile.Close()
				recordCount = 0
			}

			// Конкатенируем данные с "|" для полей с множественными значениями
			eans := strings.Join(details.Ean, "|")

			// Пишем детали в CSV
			record := []string{
				details.GUID, // Используем поле GUID для записи значения guid_potravina
				details.Name,
				details.Category,
				eans,
				details.Energy,
				details.Protein,
				details.Fat,
				details.Carb,
				details.Salt,
				details.Water,
				details.Sugar,
				details.Calcium,
				details.GI,
				details.PHE,
				details.Alcohol,
				details.Image,
			}
			if err := writer.Write(record); err != nil {
				log.Fatalf("Failed to write record to CSV: %v", err)
			} else {
				log.Printf("%v Success! PRODUCT ID: %s", recordCount, details.GUID)
			}

			recordCount++
		}
	}

	writer.Flush()
	csvFile.Close()

	log.Println("Data has been written to product_details_*.csv files")
}
