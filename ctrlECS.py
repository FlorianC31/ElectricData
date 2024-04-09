import datetime
import RPi.GPIO as GPIO
import time
import sys

RELAY_PIN = 17
FREQ = 50 # Hz

class EcsRelay():
    def __init__(self):
        self.relay_pin = RELAY_PIN
        self.freq = FREQ
        self.gpioPinInit()

    def gpioPinInit(self):

        if GPIO.getmode() is not GPIO.BCM:
            # Définir le mode de numérotation des broches GPIO (BCM)
            GPIO.setmode(GPIO.BCM)

        GPIO.setwarnings(False)
        GPIO.setup(self.relay_pin, GPIO.OUT)
        GPIO.setwarnings(True)


    def turnRelayOn(self, on):
        GPIO.input(17)

        if on == "On":
            state = GPIO.LOW
        else:
            state = GPIO.HIGH

        if state != GPIO.input(self.relay_pin):
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with open('/home/florian/enedis/relayECS.log', 'a') as fichier:
                if on == "On":
                    ligne = timestamp  + " - Turn ECS on"
                else:
                    ligne = timestamp  + " - Turn ECS off"
                    
                fichier.write(ligne + '\n')

            GPIO.output(self.relay_pin, state)
            time.sleep(1 / self.freq / 4)        # wait 1/4 of AC cycle to be sure that relays state is changed at least 
            GPIO.output(self.relay_pin, state)


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Utilisation : python mon_script.py <argument>")
    else:
        argument = sys.argv[1]
        ecs_relay = EcsRelay()
        ecs_relay.turnRelayOn(argument)
