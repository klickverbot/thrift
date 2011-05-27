/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */

#include <cassert>

#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <sys/stat.h>

#include "platform.h"
#include "t_oop_generator.h"
using namespace std;


/**
 * D code generator.
 */
class t_d_generator : public t_oop_generator {
 public:
  t_d_generator(
      t_program* program,
      const std::map<string, string>& parsed_options,
      const string& option_string)
    : t_oop_generator(program)
  {
    (void) parsed_options;
    (void) option_string;
    out_dir_base_ = "gen-d";
  }

  /**
   * Init and close methods
   */

  void init_generator() {
    // Make output directory
    MKDIR(get_out_dir().c_str());

    string dir = program_->get_namespace("d");
    string subdir = get_out_dir();
    string::size_type loc;
    while ((loc = dir.find(".")) != string::npos) {
      subdir = subdir + "/" + dir.substr(0, loc);
      MKDIR(subdir.c_str());
      dir = dir.substr(loc+1);
    }
    if (!dir.empty()) {
      subdir = subdir + "/" + dir;
      MKDIR(subdir.c_str());
    }

    package_dir_ = subdir + "/";

    // Make output file
    string f_types_name = package_dir_ + program_name_ + "_types.d";
    f_types_.open(f_types_name.c_str());

    // Print header
    f_types_ <<
      autogen_comment() <<
      "module " << get_package(*program_) << program_name_ << "_types;" << endl <<
      endl <<
      "import thrift.base;" << endl <<
      "import thrift.codegen;" << endl <<
      endl;

    // Include type modules from other imported programs.
    const vector<t_program*>& includes = program_->get_includes();
    for (size_t i = 0; i < includes.size(); ++i) {
      f_types_ <<
        "import " << get_package(*(includes[i])) <<
        includes[i]->get_name() << "_types;" << endl;
    }
    if (!includes.empty) f_types_ << endl;
  }

  void close_generator() {
    // Close output file
    f_types_.close();
  }

  void generate_consts(std::vector<t_const*> consts) {
    if (!consts.empty()) {
      string f_consts_name = package_dir_+program_name_+"_constants.d";
      ofstream f_consts;
      f_consts.open(f_consts_name.c_str());

      f_consts <<
        autogen_comment() <<
        "module " << get_package(*program_) << program_name_ << "_constants;" << endl;

      f_consts <<
        endl <<
        "import " << get_package(*get_program()) << program_name_ << "_types;" << endl <<
        endl;

      vector<t_const*>::iterator c_iter;
      for (c_iter = consts.begin(); c_iter != consts.end(); ++c_iter) {
        string name = (*c_iter)->get_name();
        t_type* type = (*c_iter)->get_type();
        f_consts <<
          indent() << type_name(type) << " " << name << ";" << endl;
      }

      f_consts <<
        endl <<
        "static this() {" << endl;
      indent_up();
      for (c_iter = consts.begin(); c_iter != consts.end(); ++c_iter) {
        print_const_value(f_consts, (*c_iter)->get_name(), (*c_iter)->get_type(),
          (*c_iter)->get_value());
      }
      indent_down();
      indent(f_consts) <<
        "}" << endl;
    }
  }

  void generate_typedef(t_typedef* ttypedef) {
    f_types_ <<
      indent() << "alias " << type_name(ttypedef->get_type()) << " " <<
      ttypedef->get_symbolic() << ";" << endl << endl;
  }

  void generate_enum(t_enum* tenum) {
    vector<t_enum_value*> constants = tenum->get_constants();

    string enum_name = tenum->get_name();
    f_types_ <<
      indent() << "enum " << enum_name;

    generate_enum_constant_list(f_types_, constants, "", "", true);

    f_types_ << endl;
  }

  void generate_struct(t_struct* tstruct) {
    generate_struct_definition(f_types_, tstruct, false);
  }
  void generate_xception(t_struct* txception) {
    generate_struct_definition(f_types_, txception, true);
  }

  void generate_service(t_service* tservice) {
    string svc_name = tservice->get_name();

    // Service implementation file includes
    string f_servicename = package_dir_ + svc_name + ".d";
    std::ofstream f_service;
    f_service.open(f_servicename.c_str());
    f_service <<
      autogen_comment() <<
      "module " << get_package(*program_) << svc_name << ";" << endl <<
      endl <<
      "import thrift.base;" << endl <<
      "import thrift.codegen;" << endl <<
      endl;

    f_service << "import " << get_package(*get_program()) << program_name_ <<
      "_types;" << endl;

    t_service* extends_service = tservice->get_extends();
    if (extends_service != NULL) {
      f_service <<
        "import " << get_package(*(extends_service->get_program())) <<
        extends_service->get_name() << ";" << endl;
    }

    f_service << endl;

    string extends = "";
    if (tservice->get_extends() != NULL) {
      extends = " : " + type_name(tservice->get_extends());
    }

    f_service <<
      indent() << "interface " << svc_name << extends << " {" << endl;
    indent_up();

    // Collect all the exception types service methods can throw so we can
    // emit the necessary aliases later.
    set<t_type*> exception_types;

    vector<t_function*> functions = tservice->get_functions();
    vector<t_function*>::iterator fn_iter;
    for (fn_iter = functions.begin(); fn_iter != functions.end(); ++fn_iter) {
      indent(f_service) << type_name((*fn_iter)->get_returntype()) <<
        " " << (*fn_iter)->get_name() << "(";

      const vector<t_field*>& fields = (*fn_iter)->get_arglist()->get_members();
      vector<t_field*>::const_iterator f_iter;
      bool first = true;
      for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
        if (first) {
          first = false;
        } else {
          f_service << ", ";
        }
        f_service << type_name((*f_iter)->get_type()) << " " << (*f_iter)->get_name();
      }

      f_service << ");" << endl;

      const vector<t_field*>& exceptions = (*fn_iter)->get_xceptions()->get_members();
      vector<t_field*>::const_iterator ex_iter;
      for (ex_iter = exceptions.begin(); ex_iter != exceptions.end(); ++ex_iter) {
        exception_types.insert((*ex_iter)->get_type());
      }
    }

    // Alias the exception types into the current scope.
    if (!exception_types.empty()) f_service << endl;
    set<t_type*>::const_iterator et_iter;
    for (et_iter = exception_types.begin(); et_iter != exception_types.end(); ++et_iter) {
      indent(f_service) << "alias " <<
        (*et_iter)->get_program()->get_namespace("d") << "." <<
        (*et_iter)->get_program()->get_name() << "_types" << "." <<
        (*et_iter)->get_name() << " " << (*et_iter)->get_name() << ";" << endl;
    }

    // Write the method metadata.
    ostringstream meta;
    indent_up();
    bool first = true;
    for (fn_iter = functions.begin(); fn_iter != functions.end(); ++fn_iter) {
      if ((*fn_iter)->get_arglist()->get_members().empty() &&
        (*fn_iter)->get_xceptions()->get_members().empty() &&
        !(*fn_iter)->is_oneway()) {
        continue;
      }

      if (first) {
        first = false;
      } else {
        meta << ",";
      }

      meta << endl <<
        indent() << "TMethodMeta(`" << (*fn_iter)->get_name() << "`, " << endl;
      indent_up();
      indent(meta) << "[";

      bool first = true;
      const vector<t_field*> &params = (*fn_iter)->get_arglist()->get_members();
      vector<t_field*>::const_iterator p_iter;
      for (p_iter = params.begin(); p_iter != params.end(); ++p_iter) {
        if (first) {
          first = false;
        } else {
          meta << ", ";
        }

        meta << "TParamMeta(`" << (*p_iter)->get_name() << "`, " << (*p_iter)->get_key();

        t_const_value* cv = (*p_iter)->get_value();
        if (cv != NULL) {
          t_type* t = get_true_type((*p_iter)->get_type());
          meta << ", `" <<
            render_const_value(meta, (*p_iter)->get_name(), t, cv) << "`";
        }
        meta << ")";
      }

      meta << "]";

      if (!(*fn_iter)->get_xceptions()->get_members().empty() ||
        (*fn_iter)->is_oneway()) {
        meta << "," << endl <<
          indent() << "[";

        bool first = true;
        const vector<t_field*>& exceptions =
          (*fn_iter)->get_xceptions()->get_members();
        vector<t_field*>::const_iterator ex_iter;
        for (ex_iter = exceptions.begin(); ex_iter != exceptions.end(); ++ex_iter) {
          if (first) {
            first = false;
          } else {
            meta << ", ";
          }

          meta << "TExceptionMeta(`" << (*ex_iter)->get_name() <<
            "`, " << (*ex_iter)->get_key() << ", `" <<
            (*ex_iter)->get_type()->get_name() << "`)";
        }

        meta << "]";
      }

      if ((*fn_iter)->is_oneway()) {
        meta << "," << endl <<
          indent() << "TMethodType.ONEWAY";
      }

      indent_down();
      meta << endl <<
        indent() << ")";
    }
    indent_down();

    if (!meta.str().empty()) {
      f_service << endl <<
        indent() << "enum methodMeta = [" << meta.str() << endl <<
        indent() << "];" << endl;
    }

    indent_down();
    indent(f_service) << "}" << endl;
  }

 private:
  void generate_struct_definition(ofstream& out, t_struct* tstruct, bool is_exception) {
    // Get members
    const vector<t_field*>& members = tstruct->get_members();

    // Open struct def
    if (is_exception) {
      indent(out) << "class " << tstruct->get_name() << " : TException {" << endl;
    } else {
      indent(out) << "struct " << tstruct->get_name() << " {" << endl;
    }
    indent_up();

    // Declare all fields
    vector<t_field*>::const_iterator m_iter;
    for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
      indent(out) << type_name((*m_iter)->get_type()) << " " <<
        (*m_iter)->get_name() << ";" << endl;
    }

    indent(out) << endl;
    indent(out) << "mixin TStructHelpers!(";

    if (!members.empty()) {
      out << "[";
      indent_up();

      bool first = true;
      vector<t_field*>::const_iterator m_iter;
      for (m_iter = members.begin(); m_iter != members.end(); ++m_iter) {
        if (first) {
          first = false;
        } else {
          out << ",";
        }
        out << endl;

        indent(out) << "TFieldMeta(`" << (*m_iter)->get_name() << "`, " <<
          (*m_iter)->get_key();

        t_const_value* cv = (*m_iter)->get_value();
        t_field::e_req req = (*m_iter)->get_req();
        if (req != t_field::T_OPT_IN_REQ_OUT || cv != NULL) {
          out << ", " << render_req(req);
        }
        if (cv != NULL) {
          t_type* t = get_true_type((*m_iter)->get_type());
          out << ", `" << render_const_value(out, (*m_iter)->get_name(), t, cv)
            << "`";
        }
        out << ")";
      }

      indent_down();
      out << endl << indent() << "]";
    }

    out << ");" << endl;

    indent_down();
    indent(out) <<
      "}" << endl <<
      endl;
  }

  void print_const_value(ostream& out, string name, t_type* type, t_const_value* value) {
    type = get_true_type(type);
    if (type->is_base_type()) {
      string v2 = render_const_value(out, name, type, value);
      indent(out) << name << " = " << v2 << ";" << endl <<
        endl;
    } else if (type->is_enum()) {
      indent(out) << name << " = cast(" << type_name(type) << ")" << value->get_integer() << ";" << endl <<
        endl;
    } else if (type->is_struct() || type->is_xception()) {
      indent(out) << name << " = " << (type->is_xception() ? "new " : "") << type_name(type) << "();" << endl;
      const vector<t_field*>& fields = ((t_struct*)type)->get_members();
      vector<t_field*>::const_iterator f_iter;
      const map<t_const_value*, t_const_value*>& val = value->get_map();
      map<t_const_value*, t_const_value*>::const_iterator v_iter;
      for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
        t_type* field_type = NULL;
        for (f_iter = fields.begin(); f_iter != fields.end(); ++f_iter) {
          if ((*f_iter)->get_name() == v_iter->first->get_string()) {
            field_type = (*f_iter)->get_type();
          }
        }
        if (field_type == NULL) {
          throw "type error: " + type->get_name() + " has no field " + v_iter->first->get_string();
        }
        string val = render_const_value(out, name, field_type, v_iter->second);
        indent(out) << name << ".set!`" << v_iter->first->get_string() << "`(" << val << ");" << endl;
      }
      out << endl;
    } else if (type->is_map()) {
      t_type* ktype = ((t_map*)type)->get_key_type();
      t_type* vtype = ((t_map*)type)->get_val_type();
      const map<t_const_value*, t_const_value*>& val = value->get_map();
      map<t_const_value*, t_const_value*>::const_iterator v_iter;
      for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
        string key = render_const_value(out, name, ktype, v_iter->first);
        string val = render_const_value(out, name, vtype, v_iter->second);
        indent(out) << name << "[" << key << "] = " << val << ";" << endl;
      }
      out << endl;
    } else if (type->is_list()) {
      t_type* etype = ((t_list*)type)->get_elem_type();
      const vector<t_const_value*>& val = value->get_list();
      vector<t_const_value*>::const_iterator v_iter;
      for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
        string val = render_const_value(out, name, etype, *v_iter);
        indent(out) << name << " ~= " << val << ";" << endl;
      }
      out << endl;
    } else if (type->is_set()) {
      t_type* etype = ((t_set*)type)->get_elem_type();
      const vector<t_const_value*>& val = value->get_list();
      vector<t_const_value*>::const_iterator v_iter;
      for (v_iter = val.begin(); v_iter != val.end(); ++v_iter) {
        string val = render_const_value(out, name, etype, *v_iter);
        indent(out) << name << " ~= " << val << ";" << endl;
      }
      out << endl;
    } else {
      throw "INVALID TYPE IN print_const_value: " + type->get_name();
    }
  }

  string render_const_value(ostream& out, string name, t_type* type, t_const_value* value) {
    (void) name;
    std::ostringstream render;

    if (type->is_base_type()) {
      t_base_type::t_base tbase = ((t_base_type*)type)->get_base();
      switch (tbase) {
      case t_base_type::TYPE_STRING:
        render << '"' << get_escaped_string(value) << '"';
        break;
      case t_base_type::TYPE_BOOL:
        render << ((value->get_integer() > 0) ? "true" : "false");
        break;
      case t_base_type::TYPE_BYTE:
      case t_base_type::TYPE_I16:
      case t_base_type::TYPE_I32:
        render << value->get_integer();
        break;
      case t_base_type::TYPE_I64:
        render << value->get_integer() << "L";
        break;
      case t_base_type::TYPE_DOUBLE:
        if (value->get_type() == t_const_value::CV_INTEGER) {
          render << value->get_integer();
        } else {
          render << value->get_double();
        }
        break;
      default:
        throw "compiler error: no const of base type " + t_base_type::t_base_name(tbase);
      }
    } else if (type->is_enum()) {
      render << "(" << type_name(type) << ")" << value->get_integer();
    } else {
      string t = tmp("tmp");
      indent(out) << type_name(type) << " " << t << ";" << endl;
      print_const_value(out, t, type, value);
      render << t;
    }

    return render.str();
  }

  /**
   * Returns the include prefix to use for a file generated by program, or the
   * empty string if no include prefix should be used.
   */
  string get_package(const t_program& program) const {
    string package = program.get_namespace("d");
    if (package.size() == 0) return "";
    return package + ".";
  }

  string type_name(t_type* ttype) {
    if (ttype->is_base_type()) {
      return base_type_name(((t_base_type*)ttype)->get_base());
    }

    if (ttype->is_container()) {
      string cname;

      t_container* tcontainer = (t_container*) ttype;
      if (tcontainer->has_cpp_name()) {
        cname = tcontainer->get_cpp_name();
      } else if (ttype->is_map()) {
        t_map* tmap = (t_map*) ttype;
        cname = type_name(tmap->get_val_type()) + "[" +
          type_name(tmap->get_key_type()) + "]";
      } else if (ttype->is_set()) {
        t_set* tset = (t_set*) ttype;
        cname = "HashSet!(" + type_name(tset->get_elem_type()) + ")";
      } else if (ttype->is_list()) {
        t_list* tlist = (t_list*) ttype;
        cname = type_name(tlist->get_elem_type()) + "[]";
      }

      return cname;
    }

    return ttype->get_name();
  }

  string render_req(t_field::e_req req) {
    switch (req) {
    case t_field::T_OPT_IN_REQ_OUT:
      return "TReq.OPT_IN_REQ_OUT";
    case t_field::T_OPTIONAL:
      return "TReq.OPTIONAL";
    case t_field::T_REQUIRED:
      return "TReq.REQUIRED";
    default:
      throw "Compiler error: Invalid requirement level: " + req;
    }
  }

  string base_type_name(t_base_type::t_base tbase) {
    switch (tbase) {
    case t_base_type::TYPE_VOID:
      return "void";
    case t_base_type::TYPE_STRING:
      return "string";
    case t_base_type::TYPE_BOOL:
      return "bool";
    case t_base_type::TYPE_BYTE:
      return "byte";
    case t_base_type::TYPE_I16:
      return "short";
    case t_base_type::TYPE_I32:
      return "int";
    case t_base_type::TYPE_I64:
      return "long";
    case t_base_type::TYPE_DOUBLE:
      return "double";
    default:
      throw "compiler error: no D base type name for base type " + t_base_type::t_base_name(tbase);
    }
  }

  void generate_enum_constant_list(std::ofstream& f,
    const vector<t_enum_value*>& constants, const char* prefix,
    const char* suffix, bool include_values)
  {
    f << " {" << endl;
    indent_up();

    vector<t_enum_value*>::const_iterator c_iter;
    bool first = true;
    for (c_iter = constants.begin(); c_iter != constants.end(); ++c_iter) {
      if (first) {
        first = false;
      } else {
        f << "," << endl;
      }
      indent(f)
        << prefix << (*c_iter)->get_name() << suffix;
      if (include_values && (*c_iter)->has_value()) {
        f << " = " << (*c_iter)->get_value();
      }
    }

    f << endl;
    indent_down();
    indent(f) << "}" << endl;
  }

  /*
   * File streams, stored here to avoid passing them as parameters to every
   * function.
   */
  ofstream f_types_;
  ofstream f_header_;

  string package_dir_;
};

THRIFT_REGISTER_GENERATOR(d, "D", "")
