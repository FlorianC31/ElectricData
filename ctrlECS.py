import datetime
import RPi.GPIO as GPIO
import time
import sys


RELAY_PIN = 17
FREQ = 50 # Hz

def gpioPinInit():

    if GPIO.getmode() is not GPIO.BCM:
        # Définir le mode de numérotation des broches GPIO (BCM)
        GPIO.setmode(GPIO.BCM)

    GPIO.setwarnings(False)
    GPIO.setup(RELAY_PIN, GPIO.OUT)
    GPIO.setwarnings(True)


def turnRelayOn(on):

    GPIO.input(RELAY_PIN)

    if on == "On":
        state = GPIO.LOW
    else:
        state = GPIO.HIGH

    if state != GPIO.input(RELAY_PIN):
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open('/home/florian/enedis/relayECS.log', 'a') as fichier:
            if on == "On":
                ligne = timestamp  + " - Turn ECS on"
            else:
                ligne = timestamp  + " - Turn ECS off"
                
            fichier.write(ligne + '\n')

        GPIO.output(RELAY_PIN, state)
        time.sleep(1 / FREQ / 4)        # wait 1/4 of AC cycle to be sure that relays state is changed at least 
        GPIO.output(RELAY_PIN, state)


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Utilisation : python mon_script.py <argument>")
    else:
        gpioPinInit()
        argument = sys.argv[1]
        turnRelayOn(argument)
