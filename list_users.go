package main

import (
    "encoding/json"
    "encoding/csv"
    "os"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "time"
    "sync"
)

type OrgUser struct {
    Details struct {
        Email string
        Id string
    }
    Organizations []struct {
        Organization string
        Permissions []string
    }
}

type UserListResponse struct {
    Count int
    Results []OrgUser
}

type PageRequest struct {
    IdClient http.Client
    Page int
    PageSize int
    IdentityHost string
    Token string
    ApplicationNamespace string
}

type UserEntry struct {
    UserEmail string
    UserId string
    OrgId string
    Permissions []string
}

func (u UserEntry) stringArray() []string {
    var retString []string
    retString = append(retString, u.UserEmail)
    retString = append(retString, u.UserId)
    retString = append(retString, u.OrgId)
    retString = append(retString, u.Permissions...)
    return retString
}

func getNumPages(idClient http.Client, identityHost string, token string, pageSize int) int {
    url := fmt.Sprintf("%s/identity/v3/users/?page=1&page_size=%s", identityHost, pageSize)

    req, err := http.NewRequest(http.MethodGet, url, nil)
    if err != nil {
        log.Fatal(err)
    }

    req.Header.Set("Authorization", fmt.Sprintf("bearer %s", token))

    res, getErr := idClient.Do(req)
    if getErr != nil {
        log.Fatal(getErr)
    }
    defer res.Body.Close()

    body, readErr := ioutil.ReadAll(res.Body)
    if readErr != nil {
        log.Fatal(readErr)
    }

    userList := UserListResponse{}
    jsonErr := json.Unmarshal(body, &userList)
    if jsonErr != nil {
        log.Fatal(jsonErr)
    }

    pageCount := userList.Count / pageSize + 2

    if userList.Count % pageCount == 0 {
        pageCount += 1
    }

    return pageCount
}

func getUserList(userQueue chan UserEntry, request PageRequest) {
    url := fmt.Sprintf("%s/identity/v3/users/?page=%d&page_size=%d", request.IdentityHost, request.Page, request.PageSize)

    fmt.Println("Formed url: ", url)
    req, err := http.NewRequest(http.MethodGet, url, nil)
    if err != nil {
        log.Fatal(err)
    }

    req.Header.Set("Authorization", fmt.Sprintf("bearer %s", request.Token))

    res, getErr := request.IdClient.Do(req)
    if getErr != nil {
        log.Fatal(getErr)
    }
    defer res.Body.Close()

    body, readErr := ioutil.ReadAll(res.Body)
    if readErr != nil {
        log.Fatal(readErr)
    }

    userList := UserListResponse{}
    jsonErr := json.Unmarshal(body, &userList)
    if jsonErr != nil {
        log.Fatal(jsonErr)
    }

    for _, user := range userList.Results {
        for _, org := range user.Organizations {
            userQueue <- UserEntry{
                user.Details.Email,
                user.Details.Id,
                org.Organization,
                org.Permissions,
            }
        }
    }
}

func queueWorker(requestQueue chan PageRequest, userQueue chan UserEntry, wg *sync.WaitGroup) {
    for {
        request := <- requestQueue
        getUserList(userQueue, request)
        wg.Done()
    }
}

func queueWorkerSingle(request PageRequest, userQueue chan UserEntry, wg *sync.WaitGroup) {
    getUserList(userQueue, request)
    wg.Done()
}

func main() {

    identityHost := "https://dev.id.spsc.io"
    token := "insert token here"
    var requestQueue chan PageRequest = make(chan PageRequest)
    var userQueue chan UserEntry = make(chan UserEntry, 2000)
    pageSize := 50
    usePool := true

    idClient := http.Client{
        Timeout: time.Second * 20,
    }

    pageCount := getNumPages(idClient, identityHost, token, pageSize)
    var wg sync.WaitGroup
    wg.Add(pageCount)

    if usePool {
        for i := 0; i < 10; i++ {
            go queueWorker(requestQueue, userQueue, &wg)
        }

        for i := 0; i < pageCount; i++ {
            requestQueue <- PageRequest{
                idClient,
                i,
                pageSize,
                identityHost,
                token,
                "",
            }
        }
    } else {
        for i := 0; i < pageCount; i++ {
            request := PageRequest{
                idClient,
                i,
                pageSize,
                identityHost,
                token,
                "",
            }
            go queueWorkerSingle(request, userQueue, &wg)
        }
    }

    wg.Wait()

    file, err := os.Create("result.csv")
    if err != nil {
        log.Fatal("Unable to create file", err)
    }
    defer file.Close()
    writer := csv.NewWriter(file)
    defer writer.Flush()

    fmt.Println("Writing csv file")
    err = writer.Write([]string{"user email","user id","org id","permissions"})
    if err != nil {
        log.Fatal("Unable to write headers to csv", err)
    }
    for len(userQueue) > 0 {
        user := <- userQueue

        err := writer.Write(user.stringArray())
        if err != nil {
            log.Fatal("Unable to write user to csv", err)
        }
    }
    fmt.Println("Done!")
}
