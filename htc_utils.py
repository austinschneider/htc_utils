import glob
import ntpath
import inspect
import argparse
from functools import wraps

class Bunch:
    def __init__(self,**kw):
        self.__dict__ = dict(kw)
        self.__setitem__ = self.__dict__.__setitem__

def param_transform(param, string_quotes=True):
    if string_quotes:
        quote = '"' 
    else:
        quote = ''
    if type(param) is int:
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

class condor_file:
    def __init__(self):
        self.buffer = []
        self.redo_buffer = []
        return

    def undo(self):
        if len(self.buffer > 0):
            self.redo_buffer.append(self.buffer.pop())

    def redo(self):
        if len(self.redo_buffer > 0):
            self.buffer.append(self.redo_buffer.pop())

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

    queue_parser0 = argparse.ArgumentParser(description='Process queue arguments')
    queue_parser0.add_argument('intexpr', metavar='intexpr', nargs='?', default='1', type=int)

    queue_parser1 = argparse.ArgumentParser(description='Process queue arguments')
    queue_parser1.add_argument('intexpr', metavar='intexpr', nargs='?', default='1', type=int)
    queue_parser1.add_argument('varname', metavar='varname', nargs='?')

    queue_parser2 = argparse.ArgumentParser(description='Process queue arguments')
    queue_parser2.add_argument('intexpr', metavar='intexpr', nargs='?', default='1', type=int)
    queue_parser2.add_argument('varnames', metavar='varnames', nargs='*')

    @buffer
    def queue(self, *args):
        n = len(args)
        if n == 0:
            return "queue"
        elif n == 1:
            return "queue %d" % args[0]

class dagman_file:
    def __init__(self) :
        self.buffer = []
        self.redo_buffer = []
        return

    def undo(self):
        if len(self.buffer > 0):
            self.redo_buffer.append(self.buffer.pop())

    def redo(self):
        if len(self.redo_buffer > 0):
            self.buffer.append(self.redo_buffer.pop())

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
                ret += " ".join(var_strings)
            except:
                raise
        else:
            var_strings = []
            for i in xrange(min(len(variables), len(values))):
                v = ""
                v += param_transform(variables[i], string_quotes=False)
                v += "="
                v += param_transform(values[i], string_quotes=True)
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
        ret = "SPLICE " + splice_name + " " + dag_filename
        if dir is not None:
            ret += " DIR " + dir
        return ret

    @buffer
    @stringify(0, 1, 1, 1, 0)
    def final(self, job_name, submit_file, dir=None, noop=False):
        ret = "FINAL " + job_name + " " + submit_file
        if dir is not None:
            ret += " DIR " + dir
        if noop:
            ret += " NOOP"
        return ret

