# Declaration of variables
CC = mpicc

SRCDIR = src
BUILDDIR = build
EXEC = bin/tsv_hist.out

SRCEXT = c

DEBUG_FLAG = #-g
OPTI_FLAGS = -O3
WARN_FLAGS =
CC_FLAGS = $(DEBUG_FLAG) $(OPTI_FLAGS) $(WARN_FLAGS) #-fsanitize=address,undefined

# File names
SOURCES = $(shell find $(SRCDIR) -type f -name *.$(SRCEXT))
OBJECTS = $(patsubst $(SRCDIR)/%,$(BUILDDIR)/%,$(SOURCES:.$(SRCEXT)=.o))

HEADERS = $(wildcard src/*.h)
LIBFLAGS = #-lprofiler

all: $(EXEC)

# Main target
$(EXEC): $(OBJECTS)
	$(CC) $^ -o $(EXEC) $(LIBFLAGS)

# To obtain object files
$(BUILDDIR)/%.o: $(SRCDIR)/%.$(SRCEXT)
	@mkdir -p $(BUILDDIR);
	$(CC) $(CC_FLAGS) -c -o $@ $<

# To remove generated files
clean:
	@echo "Cleaning...";
	rm -r $(EXEC) $(BUILDDIR)

.PHONY: clean

