import csv
import requests


#my_token = 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

def get_repo_list():

    # change these things for your org
    org_name = 'v3'
    pages_to_fetch = 2 # number of repos you have, divided by 100. kinda hack-y!

    api_url = 'https://developer.github.com/v3/repos/#list-all-repositories' .format(org_name)
    api_url = 'https://docs.github.com/en/rest/search#search-repositories' .format(org_name)
    
    
    repos_list = []

    fields = ['name',
              'html_url',
              'stargazers_count',
              'forks',
              'description',
              'homepage',
              'language',
              'created_at',
              'updated_at']

    for page in range(1,(pages_to_fetch + 1)):
        print("Page %s" % page)
        repos = requests.get(api_url + "&page=" + str(page))
        if repos.status_code == 200:
            for r in repos.json():
                repos_list.append([r[key] for key in fields])

    print("Repo list created. Length: %s" % len(repos_list))

    outp = open(('%s_repos.csv' .format(org_name)), 'w')
    writer = csv.writer(outp)
    writer.writerow(fields) # header
    writer.writerows(repos_list) # data
    outp.close()

    print("File written: %s" % ('%s_repos.csv' .format(org_name)))
    print("done")

if __name__ == "__main__":
    get_repo_list()