import requests
from urllib.parse import quote

def sendNotif(title, content, url):

    key = "k-88db4256871c"
    server = "http://xdroid.net/api/message"

    # Encoder les chaînes pour l'URL
    title_encoded = quote(title)
    contents_encoded = quote(content)
    url_encoded = quote(url)

    # Envoyer la requête POST
    response = requests.post(f"{server}?k={key}&t={title_encoded}&c={contents_encoded}&u={url_encoded}")

    return (response.status_code == 200)


if __name__ == "__main__":
    title = "Test Notification"
    content = "Hello world !"
    url = "https://flo-pictures.synology.me/photo"

    print(sendNotif(title, content, url))
