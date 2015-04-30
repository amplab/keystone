
VLFEATDIR=$(TMPDIR)/vlfeat
ENCEVALDIR=$(TMPDIR)/enceval

VLFEATURL = "http://www.vlfeat.org/download/vlfeat-0.9.20-bin.tar.gz"
ENCEVALURL = "http://www.robots.ox.ac.uk/~vgg/software/enceval_toolkit/downloads/enceval-toolkit-1.1.tar.gz"

CC = g++
CFLAGS = -O2

#Auto-detect architecture
Darwin_x86_64_ARCH := maci64
Linux_x86_64_ARCH := glnxa64

UNAME := $(shell uname -sm)
VLARCH ?= $($(shell echo "$(UNAME)" | tr \  _)_ARCH)

VLFEATOBJ = $(VLFEATDIR)/vlfeat-0.9.20/bin/$(VLARCH)/objs

#Set dynamic lib extension for architecture
Darwin_x86_64_EXT := jnilib
Linux_x86_64_EXT := so

SOEXT ?= $($(shell echo "$(UNAME)" | tr \  _)_EXT)

#Set java extension for architecture
Darwin_x86_64_JAVA := darwin
Linux_x86_64_JAVA := linux

JAVAEXT ?= $($(shell echo "$(UNAME)" | tr \  _)_JAVA)

SRCDIR := src/main/cpp

ODIR = $(TMPDIR)/obj
LDIR = lib

_OBJ = siftExtractor.o fisherExtractor.o
OBJ = $(patsubst %,$(ODIR)/%,$(_OBJ))

_EVDEPS = gmm.o fisher.o stat.o simd_math.o
EVDEPS = $(patsubst %,$(ENCEVALDIR)/%,$(_EVDEPS))

VLDEPS = $(shell find $(VLFEATOBJ) -type f -name '*.o')

all: $(LDIR)/libImageEncoders.$(SOEXT)

$(VLFEATDIR):
	mkdir -p $(VLFEATDIR)
	wget $(VLFEATURL) -O $(VLFEATDIR)/vlfeat.tgz
	cd $(VLFEATDIR) && tar zxvf $(VLFEATDIR)/vlfeat.tgz

$(ENCEVALDIR):
	mkdir -p $(ENCEVALDIR)
	wget $(ENCEVALURL) -O $(ENCEVALDIR)/enceval.tgz
	cd $(ENCEVALDIR) && tar zxvf enceval.tgz

vlfeat: $(VLFEATDIR)
	make -C $(VLFEATDIR)/vlfeat-0.9.20 ARCH=$(VLARCH) bin-all

$(ENCEVALDIR)/%.o: $(ENCEVALDIR)/lib/gmm-fisher/%.cxx
	$(CC) -c -o $@ $< $(CFLAGS)

$(ODIR)/%.o: $(SRCDIR)/%.cxx $(ENCEVALDIR) $(VLFEATDIR)
	$(CC) -I$(ENCEVALDIR)/lib/gmm-fisher -I$(VLFEATDIR)/vlfeat-0.9.20 -I$(JAVA_HOME)/include/ -I$(JAVA_HOME)/include/$(JAVAEXT) -c -o $@ $< $(CFLAGS)

$(LDIR)/libImageEncoders.$(SOEXT): $(OBJ) $(EVDEPS) vlfeat
	$(CC) -dynamiclib -o $@ $(OBJ) $(EVDEPS) $(VLDEPS) $(CFLAGS)

.PHONY: clean vlfeat

clean:
	rm -f $(LDIR)/libImageEncoder.$(SOEXT)
	rm -rf $(VLFEATDIR) $(ENCEVALDIR) $(ODIR)
