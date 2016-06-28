import requests

badz = ["http://example.com/badz/badz.php",
        "http://example.com/badz/",
        "http://www.example.com/badz/phishing.php"
        ]

# url  = 'https://api.ote-godaddy.com/v1/abuse/tickets'
url = 'https://api.godaddy.com/v1/abuse/tickets'
ote_key = 'VUjQoQwS_Enrf9hAuZFRyftGJgZnr38'
ote_secret = 'EnrjEF3RUUegiNrtVrEL9t'
prod_key = 'dKYhxgeKX99R_RiafF8nRLAvupbCPrZhPcp'
prod_secret = 'RiaimCb4aSDPU58jdeHDmW'
prod_shopper = 128777828
ote_shopper = 907951
head = {'Content-Type': 'application/json', 'Authorization': 'sso-key ' + prod_key + ':' + prod_secret}


def display(r):
    print "CONTENT:\t\t{}".format(r.content)


def post_ticket():
    for bad in badz:

        payload = {
            "type": "PHISHING",
            "source": bad,
            "info": "Netcraft Escalation"
        }

        r = requests.post(url, verify=False, json=payload, headers=head)
        print "\n\nPOST:"
        print r
        display(r)


if __name__ == '__main__':
    post_ticket()
    print "\n\n"
