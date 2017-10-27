import re
import os
import glob
import random
import ntpath
import inspect
import argparse
from functools import wraps

_animals = map(lambda s: s.strip(), open('./animals.txt', 'r').readlines())
_adjectives = map( lambda s: s.strip(), open('./adjectives.txt', 'r').readlines())
_n_animals = len(_animals)
_n_adjectives = len(_adjectives)
_animals_dict = dict(zip(_animals, xrange(_n_animals)))
_adjectives_dict = dict(zip(_adjectives, xrange(_n_animals)))
_n_uids = _n_adjectives ** 2 * _n_animals
_uid_re = re.compile('([A-Z][a-z]+)([A-Z][a-z]+)([A-Z][a-z]+)')

def _ntnnn(n):
    n2 = int(n / _n_adjectives ** 2)
    n -= n2 * _n_adjectives ** 2
    n1 = int(n / _n_adjectives)
    n -= n1 * _n_adjectives
    n0 = n
    return (n0, n1, n2)

def _nnntn(nnn):
    n0, n1, n2 = nnn
    return n0 + _n_adjectives * n1 + _n_adjectives ** 2 * n2

def _get_n(mmm):
    m0, m1, m2 = mmm
    n0 = _adjectives_dict[m0.lower()]
    n1 = _adjectives_dict[m1.lower()]
    n2 = _animals_dict[m2.lower()]

    return _nnntn((n0, n1, n2))

def _get_s(n):
    n0, n1, n2 = _ntnnn(n)
    m0 = _adjectives[n0]
    m1 = _adjectives[n1]
    m2 = _animals[n2]

    caps = lambda x: x[0].upper() + x[1:]

    return caps(m0) + caps(m1) + caps(m2)

def _get_next_s(s):
    x = _get_n(_uid_re.search(s).groups())
    x = _f(x)
    while x >= _n_uids:
        x = _f(x)
    return _get_s(x)

def _g(x, n):
    return (x >> (32 - n)) & 0x1

def _f(x):
    x |= (((1 ^ _g(x,22)) ^ _g(x,2)) ^ _g(x,1)) << 32
    x = x >> 1
    return x

def gen_file_uid(directory):
    if not os.path.isdir(directory):
        directory = os.path.dirname(directory)
    search_term = directory + '/*'
    files = glob.glob(search_term)
    files.sort(key=lambda x: min(os.path.getmtime(x), os.path.getctime(x)))
    uid = None
    for f in reversed(files):
        m = _uid_re.search(os.path.basename(f))
        if m:
            try:
                uid = _get_n(m.groups())
                break
            except Exception as e:
                raise e
    if uid is None:
        uid = random.randint(1,_n_uids-1)
    while True:
        uid = _f(uid)
        while uid >= _n_uids:
            uid = _f(uid)
        s = _get_s(uid)
        if not len(glob.glob(directory + '/*' + s + '*')):
            break

    return s

def get_next_file_uid(directory, last):
    s = _get_next_s(last)
    while len(glob.glob(directory + '/*' + s + '*')):
        s = _get_next_s(s)
    return s

class Bunch:
    def __init__(self,**kw):
        self.__dict__ = dict(kw)
        self.__setitem__ = self.__dict__.__setitem__

def param_transform(param, string_quotes=True):
    if string_quotes:
        quote = '"' 
    else:
        quote = ''
    if param is None:
        return None
    elif type(param) is int:
        ret = "%d" % param
    elif type(param) is str:
        ret = "%s" % param
    elif type(param) is bool:
        ret = "%d" % int(param)
    elif type(param) is list:
        ret = str.join(" ", [param_transform(p, string_quotes=False) for p in param])
    elif type(param) is tuple:
        ret = str.join(" ", [param_transform(p, string_quotes=False) for p in param])
    else:
        ret = str(param)

    return quote + ret + quote

def condor_transform(param):
    return param_transform(param, string_quotes=False)

def buffer(f):
    @wraps(f)
    def func(self, *args, **kwargs):
        if hasattr(self, 'redo_buffer'):
            self.redo_buffer = []
        ret = f(self, *args, **kwargs)
        self.buffer.append(ret)
        return ret
    return func

def stringify(*s_mode):
    def decorator(f):
        argspec = inspect.getargspec(f)
        nargs = 0 if argspec.args is None else len(argspec.args)
        l_kwargs = 0 if argspec.defaults is None else len(argspec.defaults)
        l_args = nargs - l_kwargs
        assert(len(s_mode) <= nargs)
        mode = list(s_mode) + [0]*(nargs - len(s_mode))
        r_mode = {}
        for i in xrange(l_args, nargs):
            r_mode[argspec.args[i]] = mode[i]
        @wraps(f)
        def func(*args, **kwargs):
            args = list(args)
            for i in xrange(min(nargs, len(args))):
                if mode[i]:
                    args[i] = param_transform(args[i], string_quotes=(mode[i] >> 1))
            for arg in kwargs.keys():
                if arg in r_mode and r_mode[arg]:
                    kwargs[arg] = param_transform(kwargs[arg], string_quotes=(r_mode[arg] >> 1))
            return f(*args, **kwargs)
        return func
    return decorator

def condor_parse(f):
    if len(inspect.getargspec(f).args) ==  1:
        @wraps(f)
        def func(self):
            return f.__name__
    elif len(inspect.getargspec(f).args) == 2:
        @wraps(f)
        def func(self, arg):
            return "%s = %s" % (f.__name__, condor_transform(arg))
    else:
        @wraps(f)
        def func(self, *args):
            args = [condor_transform(arg) for arg in args]
            return f(self, *args)
    return func

class base_buffer(object):
    def __init__(self):
        self.buffer = []
        self.redo_buffer = []
        return

    def undo(self):
        """
        Undo last buffer action.
        """
        if len(self.buffer) > 0:
            self.redo_buffer.append(self.buffer.pop())

    def redo(self):
        """
        Redo last buffer action undone.
        """
        if len(self.redo_buffer) > 0:
            self.buffer.append(self.redo_buffer.pop())

    def clear(self):
        """
        Clear undo and redo buffers.
        """
        self.buffer = []
        self.redo_buffer = []

class condor_file(base_buffer):
    def __init__(self):
        super(condor_file, self).__init__()
        return

    def __str__(self):
        return str.join("\n", self.buffer)

    @buffer
    @condor_parse
    def executable(self, x): return

    @buffer
    @condor_parse
    def log(self, x): return

    @buffer
    @condor_parse
    def input(self, x): return

    @buffer
    @condor_parse
    def output(self, x): return

    @buffer
    @condor_parse
    def error(self, x): return
    
    @buffer
    @condor_parse
    def universe(self, x): return

    @buffer
    @condor_parse
    def request_memory(self, x): return

    @buffer
    @condor_parse
    def initialdir(self, x): return

    @buffer
    @condor_parse
    def requirements(self, x): return

    @buffer
    @condor_parse
    def rank(self, x): return

    @buffer
    @condor_parse
    def should_transfer_files(self, x): return

    @buffer
    @condor_parse
    def when_to_transfer_output(self, x): return

    @buffer
    def transfer_input_files(self, x):
        return "transfer_input_files = %s" % (str.join(", ", x))

    @buffer
    @condor_parse
    def request_GPUs(self, x): return

    @buffer
    @condor_parse
    def interactivejob(self, x): return

    @buffer
    def queue(self, *args):
        return ' '.join(["queue"] + [condor_transform(x) for x in args])

    @buffer
    @condor_parse
    def arguments(self, args): return

    @buffer
    @condor_parse
    def notification(self, x): return

    @buffer
    def getenv(self, x):
        return 'getenv = %s' % str(bool(x))

class dagman_file(base_buffer):
    def __init__(self):
        super(dagman_file, self).__init__()
        return

    def __str__(self):
        return str.join("\n", self.buffer)

    @buffer
    @stringify(0, 1, 1, 1, 0, 0)
    def job(self, name, file, dir=None, noop=False, done=False):
        """
        job(JobName, SubmitDescriptionFileName, dir=None, noop=False, done=False)
        JOB JobName SubmitDescriptionFileName [DIR directory] [NOOP] [DONE]
        """
        ret = "JOB " + name + " " + file
        if dir is not None:
            ret += " DIR " + dir
        if noop:
            ret += " NOOP"
        if done:
            ret += " DONE"
        return ret

    @buffer
    @stringify(0, 1, 1)
    def dependency(self, parent, child):
        """
        dependency(parent(s), child(ren))
        PARENT ... CHILD
        """
        return "PARENT " + parent + " CHILD " + child

    @buffer
    @stringify(0, 1, 1, 1, 1)
    def script_pre(self, name, script, status=None, time=None):
        """
        script_pre(JobName, ExecutableName + arguments, status=None, time=None)
        SCRIPT [DEFER status time] PRE JobName ExecutableName [arguments]
        """
        if status is not None and time is not None:
            return "SCRIPT DEFER " + status + " " + time + " PRE " + name + " " + script
        else:
            return "SCRIPT PRE " + name + " " + script
    
    @buffer
    @stringify(0, 1, 1, 1, 1)
    def script_post(self, name, script, status=None, time=None):
        """
        script_post(JobName, ExecutableName + arguments, status=None, time=None)
        SCRIPT [DEFER status time] POST JobName ExecutableName [arguments]
        """
        if status is not None and time is not None:
            return "SCRIPT DEFER " + status + " " + time + " POST " + name + " " + script
        else:
            return "SCRIPT POST " + name + " " + script
    
    @buffer
    @stringify(0, 1, 1)
    def pre_skip(self, name, exit_code):
        """
        pre_skip(JobName, exit_code)
        PRE_SKIP JobName non-zero-exit-code
        """
        return "PRE_SKIP " + name + " " + code

    @buffer
    @stringify(0, 1, 1, 1)
    def retry(self, name, n_retries, unless_exit_value=None):
        """
        retry(JobName, NumberOfRetries, unless_exit_value=None)
        RETRY JobName NumberOfRetries [UNLESS-EXIT value]
        """
        ret = "RETRY " + name + " " + n_retries
        if unless_exit_value is not None:
            ret += " UNLESS-EXIT " + unless_exit_value 

    @buffer
    @stringify(0, 1, 1, 1)
    def abort_dag_on(self, name, exit_value, dag_ret_value=None):
        """
        abort_dag_on(JobName, AbortExitValue, dag_ret_value=None)
        ABORT-DAG-ON JobName AbortExitValue [RETURN DAGReturnValue]
        """
        ret = "ABORT-DAG-ON " + name + " " + exit_value
        if dag_ret_value is not None:
            ret += " RETURN " + dag_ret_value
        return ret
        
    @buffer
    @stringify(0, 1, 0, 0)
    def vars(self, name, variables, values=None):
        """
        vars(JobName, variables, values=None)
        VARS JobName macroname="string" [macroname="string"... ]
        """
        ret = "VARS " + name + " "
        if values is None:
            try:
                keys = variables.keys()
                var_strings = []
                for key in keys:
                    v = ""
                    v += param_transform(key, string_quotes=False)
                    v += "="
                    v += param_transform(variables[key], string_quotes=True)
                    var_strings.append(v)
                ret += str.join(" ", var_strings)
            except:
                raise
        else:
            var_strings = []
            for i in xrange(min(len(variables), len(values))):
                v = ""
                v += param_transform(variables[i], string_quotes=False)
                v += "="
                v += param_transform(values[i], string_quotes=True)
                var_strings.append(v)
            ret += str.join(" ", var_strings)
        return ret

    @buffer
    @stringify(0, 1, 1)
    def priority(self, name, priority):
        """
        priority(JobName, PriorityValue)
        PRIORITY JobName PriorityValue
        """
        return "PRIORITY " + name + " " + priority

    @buffer
    @stringify(0, 1, 1)
    def category(self, job_name, category_name):
        """
        category(JobName, CategoryName)
        CATEGORY JobName CategoryName
        """
        return "CATEGORY " + job_name + " " + category_name

    @buffer
    @stringify(0, 1, 1)
    def max_jobs(self, category_name, max_jobs):
        """
        max_jobs(CategoryName, MaxJobsValue)
        MAXJOBS CategoryName MaxJobsValue
        """
        return "MAXJOBS " + category_name + " " + max_jobs

    @buffer
    @stringify(0, 1)
    def config(self, config_file):
        """
        config(ConfigFileName)
        CONFIG ConfigFileName
        """
        return "CONFIG " + config_file

    @buffer
    @stringify(0, 1, 1)
    def set_job_attr(self, attr, value):
        """
        set_job_attr(AttributeName, AttributeValue)
        SET_JOB_ATTR AttributeName=AttributeValue
        """
        return "SET_JOB_ATTR " + attr + "=" + value

    @buffer
    @stringify(0, 1, 1, 1, 0, 0, 0)
    def subdag(self, job_name, dag_file, dir=None, noop=False, done=False, external=False):
        """
        subdag(JobName, DagFileName, directory=None, noop=False, done=False, external=False)
        SUBDAG EXTERNAL JobName DagFileName [DIR directory] [NOOP] [DONE]
        """
        ret = "SUBDAG "
        if external:
            ret += "EXTERNAL "
        ret += job_name + " "
        ret += dag_file
        if dir is not None:
            ret += " DIR " + dir
        if noop:
            ret += " NOOP"
        if done:
            ret += " DONE"
        return ret

    @buffer
    @stringify(0, 1, 1, 1)
    def splice(self, splice_name, dag_filename, dir=None):
        """
        splice(SpliceName, DagFileName, dir=None)
        SPLICE SpliceName DagFileName [DIR directory]
        """
        ret = "SPLICE " + splice_name + " " + dag_filename
        if dir is not None:
            ret += " DIR " + dir
        return ret

    @buffer
    @stringify(0, 1, 1, 1, 0)
    def final(self, job_name, submit_file, dir=None, noop=False):
        """
        final(JobName, SubmitDescriptionFileName, dir=None, noop=False)
        FINAL JobName SubmitDescriptionFileName [DIR directory] [NOOP]
        """
        ret = "FINAL " + job_name + " " + submit_file
        if dir is not None:
            ret += " DIR " + dir
        if noop:
            ret += " NOOP"
        return ret

class dag_node:
    def __init__(self, name, file, dir=None, noop=False, done=False, vars={}):
        self.children = []
        self.name = name
        self.file = file
        self.dir = dir
        self.noop = noop
        self.done = done
        self.vars = vars
        return

    def ___get_child_name___(self, child):
        if type(child) is str:
            return child
        elif hasattr(child, 'name'):
            return child.name
        else:
            return None

    def add_child(self, child):
        c_name = self.___get_child_name___(child)
        if c_name is None:
            raise ValueError("Cannot add child!")
        self.children.append(child)

    def add_children(self, children):
        for child in children:
            self.add_child(child)

    def add_var(self, variable, value):
        self.vars[variable] = value

    def write_node_definition(self, dag_file):
        dag_file.job(self.name, self.file, self.dir, self.noop, self.done)
        for key in self.vars.keys():
            dag_file.vars(self.name, [key], [self.vars[key]])

    def write_node_relationships(self, dag_file):
        if len(self.children):
            dag_file.dependency(self.name, [self.___get_child_name___(child) for child in self.children])

class dagman:
    def __init__(name):
        return
