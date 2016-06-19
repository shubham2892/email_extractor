from local_constant import server, password, user
import logging

logger = logging.getLogger(__name__)
logger.basicConfig(filename='email_fetcher.log', format='%(levelname)s:%(asctime)s %(message)s',
                   datefmt='%m/%d/%Y %I:%M:%S %p')


def connect_imap_server():
    import imaplib

    mail = imaplib.IMAP4_SSL(server)
    mail.login(user, password)
    mail.select("INBOX")  # connect to inbox.
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
        email_dict = parse_email(raw_email)
        if email_dict is not None:
            is_inserted, conn = insert_database(email_dict)
            # Show the mail into showing.
            if is_inserted:
                if move_message(id):
                    conn.close()
                else:
                    logger.debug("Unable to move message:{}".format(id))
                    conn.rollback()
            else:
                logger.debug("Unable to insert into database:{}".format(id))


def move_message(msg_uid):
    """
    Move the message to "showing" inbox
    :param msg_uid:
    :return:
    """
    obj = connect_imap_server()
    apply_lbl_msg = obj.uid('COPY', msg_uid, "INBOX.Showings")
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


def insert_database(email_dict):
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

    try:
        add_salary = "INSERT INTO showings(mls, property, price, showing_agent, email, office, office_phone, " \
                     "cell_phone, type, status, date, time) VALUES (%s,'%s',%d,'%s','%s','%s','%s','%s','%s','%s','%s','%s')" \
                     % (mls, property, price, showing_agent, email, office, office_phone, cell_phone, type_text, status,
                        date_text, time_text)

        x.execute(add_salary)
        conn.commit()
    except:
        logger.debug(add_salary)
        conn.rollback()
        return False, conn

    # conn.close()
    return True, conn


def rreplace(s, old, new='', occurrence=0):
    li = s.rsplit(old)
    return new.join(li)


def parse_email(mail_object):
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
    import email
    from datetime import datetime
    email_message = email.message_from_string(mail_object)
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


if __name__ == "__main__":
    fetch_parse_move()
