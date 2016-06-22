from local_constant import server, password, user
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename='email_fetcher.log', format='%(levelname)s:%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')

LISTING = 1
SHOWINGS = 2

MAILBOX_TYPE_MAP = {LISTING: "INBOX.Showings", SHOWINGS: "INBOX.Listing_Inquiries"}


def connect_imap_server():
    import imaplib

    mail = imaplib.IMAP4_SSL(server)
    mail.login(user, password)
    mail.select("INBOX.Testing")  # connect to inbox.
    return mail


def fetch_parse_move():
    """
    - Fetches the mail ID's from IMAP server
    - Parse each email
    - Make an entry in database
    - Move the message to "showings"
    :return:
    """

    mail = connect_imap_server()
    result, data = mail.uid('search', None, 'ALL')

    ids = data[0]  # data is a list.
    id_list = ids.split()  # ids is a space separated string
    for id in id_list:
        logger.info("Starting for the maid Id:{}".format(id))
        result, data = mail.uid('fetch', id, '(RFC822)')  # fetch the email body (RFC822) for the given ID

        raw_email = data[0][1]  # here's the body, which is raw text of the whole email
        # including headers and alternate payloads
        email_string, email_type = parse_email(raw_email)
        if email_string is not None:
            is_inserted, conn = insert_database(email_string)
            # Show the mail into showing.

            if is_inserted:
                if move_message(id, email_type):
                    conn.close()
                else:
                    logger.debug("Unable to move message:{}".format(id))
                    conn.rollback()
            else:
                logger.debug("Unable to insert into database:{}".format(id))


def move_message(msg_uid, email_type):
    """
    Move the message to "showing" inbox
    :param msg_uid:
    :return:
    """
    obj = connect_imap_server()
    apply_lbl_msg = obj.uid('COPY', msg_uid, MAILBOX_TYPE_MAP[email_type])
    if apply_lbl_msg[0] == 'OK':
        mov, data = obj.uid('STORE', msg_uid, '+FLAGS', '(\Deleted)')
        if mov == 'OK':
            expunge_reponse = obj.expunge()
            if expunge_reponse[0] == 'OK':
                return True
            else:
                logger.debug("Expunge fail:{}".format(msg_uid))
            return False
        else:
            logger.debug("Delete fail:{}".format(msg_uid))
    else:
        logger.debug("Copy fail:{}".format(msg_uid))

    return False


def insert_database(insert_string):
    """
    - Insert the relevant in the "Showings" table
    :param email_dict:
    :return:
    True: If the entry was succesfully made
    False: If an error occured while making the entry
    """

    import MySQLdb

    user = "programmer-1"
    password = "4nOPupR577Z7CR07"
    db = "pyr_property"

    conn = MySQLdb.connect(host="localhost", user=user, passwd=password, db=db)
    x = conn.cursor()

    try:
        x.execute(insert_string)
        conn.commit()
    except:
        logger.debug(insert_string)
        conn.rollback()
        return False, conn

    # conn.close()
    return True, conn


def rreplace(s, old, new='', occurrence=0):
    li = s.rsplit(old)
    return new.join(li)


def parse_email_body(email_body):
    if email_body.is_multipart():
        for payload in email_body.get_payload():
            # if payload.is_multipart(): ...
            return parse_email_body(payload)
    else:
        return email_body.get_payload()


def parse_email(mail_object):
    import email
    email_message = email.message_from_string(mail_object)
    if email_message['from'] == 'HotPads <inquiries@hotpads.com>':
        logger.info("enquiry mail detected")
        email_dict = parse_enquiry_hotpad(email_message)
        return create_enquiry_insert_string(email_dict), LISTING
    elif email_message['from'] == 'newlead@homes.com':
        logger.info("homes mail detected")
        email_dict = parse_enquiry_homes(email_message)
        return create_enquiry_insert_string(email_dict), LISTING

    elif '<referrals@apartmentlist.com>' in email_message['from']:
        logger.info("apartment list mail detected")
        email_dict = parse_enquiry_apartment_list(email_message)
        return create_enquiry_insert_string(email_dict), LISTING
    elif '<updates@trulia.com>' in email_message['from']:
        logger.info("trulia mail detected")
        email_dict = parse_enquiry_trulia(email_message)
        return create_enquiry_insert_string(email_dict), LISTING
    else:
        email_dict = parse_listing(email_message)
        return create_listing_insert_string(email_dict), SHOWINGS


def parse_listing(email_message):
    """
        Parse the email text to find the relevant fields
        Sample Text:
        ['A new showing has been created or updated',
     '',
     'LISTING INFORMATION',
     '1112819',
     '10427 CATFISH LN',
     'SAN ANTONIO, TX 78224',
     '',
     'SHOWING AGENT (If Applicable)',
     'DANIEL APONTE',
     'daponte@satx.rr.com',
     'HOME TEAM OF AMERICA',
     'Office: 210-490-8000',
     'Cell: 210-663-7164',
     '',
     'SHOWING INFORMATION',
     'Type: Showing ',
     'Status: Cancelled by Agent',
     '',
     'Date: Saturday, May 09, 2015',
     'Time: 4:30 PM - 5:30 PM',
     '',
     'Total Appointment Requests for this Listing : 0',
     '',
     'For information regarding this showing, contact Centralized Showing Service at 210-222-2227.',
     'This email was generated on 5/9/2015 1:56:47 PM CT and sent to **SendToEmail**. (53250662).',
     'To unsubscribe from Showing Notification emails, login to your account, click SETTINGS, and change the setting for Auto Showing Notifications.',
     '',
     'By receiving this email and/or utilizing the contents, you agree to our Terms of Service at http://tos.showings.com/tos.htm']
                                                      ||
                                                      ||
                                                      ||
                                                    ~~~~~~
                                                    \    /
                                                     \  /
                                                      \/
      sample output:

      {'cell_phone': ' 210-663-7164',
     'date': '2015-06-09',
     'email': 'daponte@satx.rr.com',
     'mls': '1112819',
     'office': 'Home Team Of America',
     'office_phone': ' 210-490-8000',
     'property': '10427 Catfish Ln',
     'showing_agent': 'Daniel Aponte',
     'status': ' Cancelled By Agent',
     'time': ' 4:30 PM - 5:30 PM',
     'type': ' Showing '}


        :param mail_object:
        :return:
        dictionary of each field with its value
        """

    from datetime import datetime

    price = 0
    try:
        html_string = email_message.get_payload()[1].get_payload(decode=True)
        if "Price:" in html_string:
            index = html_string.find("$")
            price = int(html_string[index + 1:index + 5].strip())

        email_string_array = email_message.get_payload()[0].get_payload(decode=True).split('\r\n')
    except IndexError:
        return

    listing_string = 'LISTING INFORMATION'
    agent_string = 'SHOWING AGENT'
    index = -1
    email_dict = {"price": price}
    for email_string in email_string_array:
        index += 1
        if listing_string.lower() in email_string.lower().strip():
            index_listing = index
            email_dict['mls'] = email_string_array[index_listing + 1].title()
            email_dict['property'] = email_string_array[index_listing + 2].title()

        elif agent_string.lower() in email_string.lower().strip():
            agent_listing = index
            email_dict['showing_agent'] = email_string_array[agent_listing + 1].title()
            email_dict['email'] = email_string_array[agent_listing + 2].lower()
            email_dict['office'] = email_string_array[agent_listing + 3].title()

        elif 'Office' in email_string:
            email_dict['office_phone'] = email_string_array[index].split(":")[1]

        elif 'Cell' in email_string:
            email_dict['cell_phone'] = email_string_array[index].split(":")[1]

        elif 'Type' in email_string:
            email_dict['type'] = email_string_array[index].split(":")[1].title()

        elif 'Status' in email_string:
            email_dict['status'] = email_string_array[index].split(":")[1].title()

        elif 'Date' in email_string:
            datetime_string = email_string_array[index].split(":")[1]
            custom_date = datetime.strptime(datetime_string.strip(), '%A, %B %d, %Y')
            email_dict['date'] = custom_date.strftime('%Y-%m-%d')

        elif 'Time' in email_string:
            email_dict['time'] = email_string_array[index][5:]

    return email_dict


def create_listing_insert_string(email_dict):
    mls = email_dict.get('mls')
    property = email_dict.get('property')
    price = email_dict.get('price', 0)
    showing_agent = rreplace(email_dict.get('showing_agent'), "'")
    email = rreplace(email_dict.get('email'), "'")
    office = rreplace(email_dict.get('office'), "'")
    office_phone = email_dict.get('office_phone')
    cell_phone = email_dict.get('cell_phone')
    type_text = email_dict.get('type')
    status = email_dict.get('status')
    date_text = email_dict.get('date')
    time_text = email_dict.get('time')

    add_salary = "INSERT INTO showings(mls, property, price, showing_agent, email, office, office_phone, " \
                 "cell_phone, type, status, date, time) VALUES (%s,'%s',%d,'%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                 % (mls, property, price, showing_agent, email, office, office_phone, cell_phone, type_text, status,
                    date_text, time_text)

    return add_salary


def create_enquiry_insert_string(email_dict):
    contact_name = email_dict.get('contact_name')
    contact_email = email_dict.get('contact_email')
    contact_phone = email_dict.get('contact_phone')
    property = email_dict.get('property')
    date_time = email_dict.get('contact_time')
    source = email_dict.get('source')

    insert_listing = "INSERT INTO listinginquiries(source, contact_name, contact_email, contact_phone, contact_time," \
                     "property) VALUES ('%s','%s','%s','%s','%s','%s')" % (source, contact_name,
                                                                           contact_email, contact_phone, date_time,
                                                                           property)
    print insert_listing
    return insert_listing


def parse_enquiry_hotpad(email_message):
    from bs4 import BeautifulSoup
    import dateutil.parser

    logger.info("Parsing enquiry")
    email_string = parse_email_body(email_message)
    soup = BeautifulSoup(email_string)
    try:
        contact_name = list(soup.find_all('span', style='font-size: 0.9em; line-height: 1.0em;')[0].descendants)[2]
        contact_email = list(soup.find_all('span', style='font-size: 0.9em; line-height: 1.0em;')[0].descendants)[5][
                        1:-1]
        contact_phone_text = soup.find_all('span', style='font-size: 0.9em; line-height: 1.0em;')[1].text
        if 'Phone:' in contact_phone_text:
            contact_phone = contact_phone_text.split(":")[1]
            if '(' not in contact_phone:
                contact_phone = contact_phone.replace(" ", "")
                contact_phone = '(%s) %s-%s' % (contact_phone[0:3], contact_phone[3:6], contact_phone[6:10])
        else:
            contact_phone = ''
    except IndexError as e:
        logger.debug(soup)
        logger.error(e)
        return {}

    property = email_message['subject'][8:].strip()
    email_date_time = dateutil.parser.parse(email_message['Date']).strftime('%Y-%m-%d %H:%M:%S')
    email_dict = {'source': 'hotpads', 'contact_name': contact_name, 'contact_email': contact_email,
                  'contact_phone': contact_phone, 'contact_time': email_date_time, 'property': property}
    return email_dict


def parse_enquiry_trulia(email_message):
    import dateutil.parser
    email_string = parse_email_body(email_message)
    email_string_array = email_string.split('\r\n')
    contact_name = email_string_array[9].split(":")[1].strip()
    contact_email = email_string_array[10].split(":")[1].strip().replace(";", "")
    contact_phone = email_string_array[11].split(":")[1].strip()
    source = "Trulia"
    property = email_message['subject'][16:].strip()
    email_date_time = dateutil.parser.parse(email_message['Date']).strftime('%Y-%m-%d %H:%M:%S')
    email_dict = {'source': source, 'contact_name': contact_name, 'contact_email': contact_email,
                  'contact_phone': contact_phone, 'contact_time': email_date_time, 'property': property}

    return email_dict


def parse_enquiry_apartment_list(email_message):
    import dateutil.parser
    email_date_time = dateutil.parser.parse(email_message['Date']).strftime('%Y-%m-%d %H:%M:%S')
    email_string = parse_email_body(email_message)
    email_string_array = email_string.split('\r\n')
    property = email_message['subject'][24:].strip()
    contact_name = email_string_array[18]
    contact_phone = email_string_array[20].split(":")[1].strip()
    contact_email = email_string_array[21].split(":")[1].strip()
    if "Email" in email_string_array[20]:
        contact_email = contact_phone
        contact_phone = ""
    source = 'Apartment List'
    email_dict = {'source': source, 'contact_name': contact_name, 'contact_email': contact_email,
                  'contact_phone': contact_phone, 'contact_time': email_date_time, 'property': property}

    return email_dict


def parse_enquiry_homes(email_message):
    from bs4 import BeautifulSoup
    import dateutil.parser
    logger.info("Parsing enquiry")
    email_html = email_message.get_payload()[1].get_payload()
    soup = BeautifulSoup(email_html)
    email_string_div_array = list(soup.find_all('div')[0].descendants)
    # print email_string_div_array
    first_name = email_string_div_array[3].lstrip()
    last_name = email_string_div_array[8].rstrip()
    contact_email = email_string_div_array[18].strip()
    contact_phone = email_string_div_array[13]
    contact_phone = contact_phone.replace("-", "").strip()
    contact_phone = '(%s) %s-%s' % (contact_phone[0:3], contact_phone[3:6], contact_phone[6:10])
    source = 'homes.com'
    try:
        property = email_message.get_payload()[0].get_payload().split('\r\n\r\n')[8].split('\t\t\t\t\t\t\t')[0].strip()[
                   2:-2]
    except IndexError as e:
        logger.debug(soup)
        logger.error(e)
        return {}

    email_date_time = dateutil.parser.parse(email_message['Date']).strftime('%Y-%m-%d %H:%M:%S')
    email_dict = {'source': source, 'contact_name': first_name + last_name, 'contact_email': contact_email,
                  'contact_phone': contact_phone, 'contact_time': email_date_time, 'property': property}

    return email_dict


if __name__ == "__main__":
    fetch_parse_move()
