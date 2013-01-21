default: all

.DEFAULT:
	cd engine && $(MAKE) $@
	cd app && $(MAKE) $@
	cd bench && $(MAKE) $@
