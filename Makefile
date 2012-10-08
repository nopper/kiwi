default: all

.DEFAULT:
	cd engine && $(MAKE) $@
	cd app && $(MAKE) $@
