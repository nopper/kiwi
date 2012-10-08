import string
import random

NUM_KEYS = 1024
LEN_STR = 10

def randstr(N):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(N))

words = """chichi's
feather
glissando's
alternator
tantrum's
chaperones
following's
fraudulence's
protagonists
Vladivostok
peer's
filterable
Paleocene's
chilblain's
abolition's
italicizes
interwoven
artworks
Beltane
nowhere's
momentous
overdraft
scripture
evaporate
pole's
commodities
stalks
windburn's
insentience
slower
veterinarian
undergraduates
flipper
jester's
monthlies
Chukchi
untimeliness
climatic
Rich's
mewls
sables
adjudicates
whip
paranoids
softhearted
alchemists
meningitis's
maestro's
determinate
junction's
Liston
pout
pique's
dances
splotch
sicks
rockers
arable
desperate
defoliant's
wapiti
grocers
getaways
strokes
mollycoddling
emancipator's
chauvinist
ravenous
earfuls
originates
discussing
bumpy
trammelling
Brittney
openhanded
piddled
pharmacy's
homographs
Gilliam
incoming
breakers
deemed
given's
terrapin's
Elastoplast's
barrio's
nonsense's
crewed
pizzicati
caching
handiness's
dukedoms
parabolic
macaws
tillable
detraction
Bimini's
Bordeaux's
hammerings
scintillate"""
d = {}

# for i in range(NUM_KEYS):
# 	key, value = randstr(LEN_STR), randstr(LEN_STR)

# 	d[key] = value

for key in words.splitlines():
        value = 'diomadonna'
        d[key] = value
        print 'buffer_clear(key); buffer_clear(value);'
        print 'buffer_putnstr(key, "%s", %d);' % (key, len(key))
        print 'buffer_putnstr(value, "%s", %d);' % (value, len(value))
        print 'db_add(db, key, value);'

for key, value in d.iteritems():
        print 'buffer_clear(key); buffer_clear(value);'
        print 'buffer_putnstr(key, "%s", %d);' % (key, len(key))
        print 'db_get(db, key, value);'
        print 'if (memcmp(value->mem, "%s", %d) != 0) { int3(); printf("ERMHAGHERD: ERROR: %%.*s != %%.*s\\n", value->length, value->mem, %d, "%s");}' % (value, len(value), len(value), value)
