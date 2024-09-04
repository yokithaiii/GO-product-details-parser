package main

import (
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

type Product struct {
	ID string `json:"id"`
}

type ProductsResponse struct {
	Data []Product `json:"data"`
}

type ProductDetails struct {
	XMLName  xml.Name `xml:"potravina"`
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
	Image    string   `xml:"image"`
	Vitamins []Label  `xml:"stitkyVitaminy>stitek"`
	Minerals []Label  `xml:"stitkyMineraly>stitek"`
}

type Label struct {
	Name        string `xml:"nazev,attr"`
	Description string `xml:",chardata"`
}

func main() {
	// Открываем JSON-файл
	file, err := ioutil.ReadFile("data-0.json")
	if err != nil {
		log.Fatalf("Error reading JSON file: %v", err)
	}

	// Парсим JSON
	var productsData ProductsResponse
	if err := json.Unmarshal(file, &productsData); err != nil {
		log.Fatalf("Error parsing JSON: %v", err)
	}

	products := productsData.Data

	const recordsPerFile = 500
	fileIndex := 1
	recordCount := 0

	// Функция для создания нового CSV файла
	createCSVFile := func(fileIndex int) (*csv.Writer, *os.File) {
		fileName := fmt.Sprintf("results/product_details_%d.csv", fileIndex)
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
			"Vitamins",
			"VitaminsDescr",
			"Minerals",
			"MineralsDescr",
		}
		if err := writer.Write(headers); err != nil {
			log.Fatalf("Failed to write headers to CSV: %v", err)
		}

		return writer, csvFile
	}

	writer, csvFile := createCSVFile(fileIndex)
	defer csvFile.Close()

	// Обходим все продукты и получаем их детали
	for _, product := range products {
		if recordCount >= recordsPerFile {
			writer.Flush()
			csvFile.Close()
			fileIndex++
			writer, csvFile = createCSVFile(fileIndex)
			defer csvFile.Close()
			recordCount = 0
			continue
		}

		url := fmt.Sprintf("https://www.dine4fit.com/getPotravina.php?GUID_Potravina=%s&lang=ru&pid=iOShVbzP", product.ID)
		response, err := http.Get(url)
		if err != nil {
			log.Printf("Failed to fetch details for ID %s: %v", product.ID, err)
			continue
		}
		defer response.Body.Close()

		var details ProductDetails
		if err := xml.NewDecoder(response.Body).Decode(&details); err != nil {
			log.Printf("Failed to decode XML for ID %s: %v", product.ID, err)
			continue
		}

		// Конкатенируем данные с "|" для полей с множественными значениями
		eans := strings.Join(details.Ean, "|")

		// Конкатенируем названия и описания витаминов и минералов
		var vitaminNames, vitaminDescriptions []string
		for _, vitamin := range details.Vitamins {
			vitaminNames = append(vitaminNames, vitamin.Name)
			vitaminDescriptions = append(vitaminDescriptions, vitamin.Description)
		}
		vitamins := strings.Join(vitaminNames, "|")
		vitaminsDescr := strings.Join(vitaminDescriptions, "|")

		var mineralNames, mineralDescriptions []string
		for _, mineral := range details.Minerals {
			mineralNames = append(mineralNames, mineral.Name)
			mineralDescriptions = append(mineralDescriptions, mineral.Description)
		}
		minerals := strings.Join(mineralNames, "|")
		mineralsDescr := strings.Join(mineralDescriptions, "|")

		// Пишем детали в CSV
		record := []string{
			product.ID,
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
			vitamins,
			vitaminsDescr,
			minerals,
			mineralsDescr,
		}
		if err := writer.Write(record); err != nil {
			log.Fatalf("Failed to write record to CSV: %v", err)
		} else {
			log.Printf("%v Success! PRODUCT ID: %s", recordCount, product.ID)
		}

		recordCount++
	}

	writer.Flush()
	csvFile.Close()

	log.Println("Data has been written to product_details_*.csv files")
}
