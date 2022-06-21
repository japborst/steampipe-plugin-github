package github

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/google/go-github/v45/github"
	"golang.org/x/oauth2"

	"github.com/turbot/steampipe-plugin-sdk/v3/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin/transform"
)

// create service client
func connect(ctx context.Context, d *plugin.QueryData) *github.Client {

	// Load connection from cache, which preserves throttling protection etc
	cacheKey := "github"
	if cachedData, ok := d.ConnectionManager.Cache.Get(cacheKey); ok {
		return cachedData.(*github.Client)
	}

	token := os.Getenv("GITHUB_TOKEN")
	baseURL := os.Getenv("GITHUB_BASE_URL")

	// Get connection config for plugin
	githubConfig := GetConfig(d.Connection)
	if githubConfig.Token != nil {
		token = *githubConfig.Token
	}
	if githubConfig.BaseURL != nil {
		baseURL = *githubConfig.BaseURL
	}

	if token == "" {
		panic("'token' must be set in the connection configuration. Edit your connection configuration file and then restart Steampipe")
	}

	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)
	tc := oauth2.NewClient(ctx, ts)
	conn := github.NewClient(tc)

	// If the base URL was provided then set it on the client. Used for
	// enterprise installs.
	if baseURL != "" {
		uv3, err := url.Parse(baseURL)
		if err != nil {
			panic(fmt.Sprintf("github.base_url is invalid: %s", baseURL))
		}

		if uv3.String() != "https://api.github.com/" {
			uv3.Path = uv3.Path + "api/v3/"
		}

		// The upload URL is not set as it's not currently required
		conn, err = github.NewEnterpriseClient(uv3.String(), "", tc)
		if err != nil {
			panic(fmt.Sprintf("error creating GitHub client: %v", err))
		}

		conn.BaseURL = uv3
	}

	// Save to cache
	d.ConnectionManager.Cache.Set(cacheKey, conn)

	return conn
}

type GetGitHubDataFunc func(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData, client *github.Client, opts *github.ListOptions) (*GitHubDataReponse, error)

type GitHubDataReponse struct {
	data     interface{}
	response *github.Response
}

func getGitHubItem(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData, getDetailsFunc GetGitHubDataFunc) (*GitHubDataReponse, error) {
	getDetails := func(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
		client := connect(ctx, d)

		plugin.Logger(ctx).Trace("Hydrating", "table", d.Table.Name, "quals", d.KeyColumnQuals, "fetchType", d.FetchType)
		return getDetailsFunc(ctx, d, h, client, nil)
	}

	data, err := plugin.RetryHydrate(ctx, d, h, getDetails, &plugin.RetryConfig{ShouldRetryError: shouldRetryError})

	if err != nil {
		return nil, err
	}

	response := data.(GitHubDataReponse)

	return &response, err
}

func streamGitHubListOrItem(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData, getDetailsFunc GetGitHubDataFunc) (interface{}, error) {
	data, err := getGitHubItem(ctx, d, h, getDetailsFunc)

	if err != nil {
		return nil, err
	}

	if data != nil {
		switch t := data.(type) {
		case []interface{}:
			streamList(ctx, d, t)
		case interface{}:
			d.StreamListItem(ctx, t)
		}
	}

	return nil, nil
}

func streamGitHubList(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData, getListFunc GetGitHubDataFunc) (interface{}, error) {
	opts := &github.ListOptions{PerPage: 100}

	limit := d.QueryContext.Limit
	if limit != nil {
		if *limit < int64(opts.PerPage) {
			opts.PerPage = int(*limit)
		}
	}

	for {
		data, err := getGitHubItem(ctx, d, h, getListFunc)
		response := data.response

		if response.NextPage == 0 {
			break
		}

		opts.Page = response.NextPage
	}

	return data, err
}

func streamList(ctx context.Context, d *plugin.QueryData, item []interface{}) {
	for _, item := range item {
		if item != "" {
			d.StreamListItem(ctx, item)
		}

		// Context can be cancelled due to manual cancellation or the limit has been hit
		if d.QueryStatus.RowsRemaining(ctx) == 0 {
			break
		}
	}
}

//// HELPER FUNCTIONS

func parseRepoFullName(fullName string) (string, string) {
	owner := ""
	repo := ""
	s := strings.Split(fullName, "/")
	owner = s[0]
	if len(s) > 1 {
		repo = s[1]
	}
	return owner, repo
}

// transforms

func convertTimestamp(ctx context.Context, input *transform.TransformData) (interface{}, error) {
	switch t := input.Value.(type) {
	case *github.Timestamp:
		return t.Format(time.RFC3339), nil
	case github.Timestamp:
		return t.Format(time.RFC3339), nil
	default:
		return nil, nil
	}
}

func filterUserLogins(_ context.Context, input *transform.TransformData) (interface{}, error) {
	user_logins := make([]string, 0)
	if input.Value == nil {
		return user_logins, nil
	}

	var userType []*github.User

	// Check type of the transform values otherwise it is throwing error while type casting the interface to []*github.User type
	if reflect.TypeOf(input.Value) != reflect.TypeOf(userType) {
		return nil, nil
	}

	users := input.Value.([]*github.User)

	if users == nil {
		return user_logins, nil
	}

	for _, u := range users {
		user_logins = append(user_logins, *u.Login)
	}
	return user_logins, nil
}

func gitHubSearchRepositoryColumns(columns []*plugin.Column) []*plugin.Column {
	return append(gitHubRepositoryColumns(), columns...)
}
