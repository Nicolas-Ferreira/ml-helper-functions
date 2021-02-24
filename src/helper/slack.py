import slack


def post_message_to_slack(token, message, channel):

    """

    Post message to slack channel
    
    :param token: Slack bot token
    :param message: Message to send
    :param channel: Slack channel where to send the message
    :return: True/False

    """
    
    try:

        # Slack Client
        client = slack.WebClient(token=token)

        # Post Message
        client.chat_postMessage(
            channel=channel, 
            text=message
        )

        return True
        
    except:
        return False
