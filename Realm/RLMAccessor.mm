////////////////////////////////////////////////////////////////////////////
//
// Copyright 2014 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#import "RLMAccessor.h"

#import "RLMArray_Private.hpp"
#import "RLMObservation.hpp"
#import "RLMObjectSchema_Private.hpp"
#import "RLMObjectStore.h"
#import "RLMObject_Private.hpp"
#import "RLMProperty_Private.h"
#import "RLMRealm_Private.hpp"
#import "RLMResults_Private.h"
#import "RLMSchema_Private.h"
#import "RLMUtil.hpp"
#import "results.hpp"

#import <objc/runtime.h>
#import <realm/descriptor.hpp>

// long getter/setter
template<typename Integer>
static inline Integer RLMGetInt(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex) {
    RLMVerifyAttached(obj);
    return static_cast<Integer>(obj->_row.get_int(colIndex));
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex, long long val) {
    RLMVerifyInWriteTransaction(obj);
    obj->_row.set_int(colIndex, val);
}

// float getter/setter
static inline float RLMGetFloat(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex) {
    RLMVerifyAttached(obj);
    return obj->_row.get_float(colIndex);
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex, float val) {
    RLMVerifyInWriteTransaction(obj);
    obj->_row.set_float(colIndex, val);
}

// double getter/setter
static inline double RLMGetDouble(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex) {
    RLMVerifyAttached(obj);
    return obj->_row.get_double(colIndex);
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex, double val) {
    RLMVerifyInWriteTransaction(obj);
    obj->_row.set_double(colIndex, val);
}

// bool getter/setter
static inline bool RLMGetBool(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex) {
    RLMVerifyAttached(obj);
    return obj->_row.get_bool(colIndex);
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex, BOOL val) {
    RLMVerifyInWriteTransaction(obj);
    obj->_row.set_bool(colIndex, val);
}

// string getter/setter
static inline NSString *RLMGetString(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex) {
    RLMVerifyAttached(obj);
    return RLMStringDataToNSString(obj->_row.get_string(colIndex));
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex, __unsafe_unretained NSString *const val) {
    RLMVerifyInWriteTransaction(obj);
    try {
        obj->_row.set_string(colIndex, RLMStringDataWithNSString(val));
    }
    catch (std::exception const& e) {
        @throw RLMException(e);
    }
}
static inline void RLMSetValueUnique(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex, NSString *propName,
                                     __unsafe_unretained NSString *const val) {
    RLMVerifyInWriteTransaction(obj);
    realm::StringData str = RLMStringDataWithNSString(val);
    size_t row = obj->_row.get_table()->find_first_string(colIndex, str);
    if (row == obj->_row.get_index()) {
        return;
    }
    if (row != realm::not_found) {
        @throw RLMException(@"Can't set primary key property '%@' to existing value '%@'.", propName, val);
    }
    try {
        obj->_row.set_string(colIndex, str);
    }
    catch (std::exception const& e) {
        @throw RLMException(e);
    }
}

// date getter/setter
static inline NSDate *RLMGetDate(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex) {
    RLMVerifyAttached(obj);
    return RLMTimestampToNSDate(obj->_row.get_timestamp(colIndex));
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex, __unsafe_unretained NSDate *const date) {
    RLMVerifyInWriteTransaction(obj);
    if (date) {
        obj->_row.set_timestamp(colIndex, RLMTimestampForNSDate(date));
    }
    else {
        obj->_row.set_null(colIndex);
    }
}

// data getter/setter
static inline NSData *RLMGetData(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex) {
    RLMVerifyAttached(obj);
    realm::BinaryData data = obj->_row.get_binary(colIndex);
    return RLMBinaryDataToNSData(data);
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex, __unsafe_unretained NSData *const data) {
    RLMVerifyInWriteTransaction(obj);

    try {
        obj->_row.set_binary(colIndex, RLMBinaryDataForNSData(data));
    }
    catch (std::exception const& e) {
        @throw RLMException(e);
    }
}

static inline RLMObjectBase *RLMGetLinkedObjectForValue(__unsafe_unretained RLMRealm *const realm,
                                                        __unsafe_unretained NSString *const className,
                                                        __unsafe_unretained id const value,
                                                        RLMCreationOptions creationOptions) NS_RETURNS_RETAINED;
static inline RLMObjectBase *RLMGetLinkedObjectForValue(__unsafe_unretained RLMRealm *const realm,
                                                        __unsafe_unretained NSString *const className,
                                                        __unsafe_unretained id const value,
                                                        RLMCreationOptions creationOptions) {
    RLMObjectBase *link = RLMDynamicCast<RLMObjectBase>(value);
    if (!link || ![link->_objectSchema.className isEqualToString:className]) {
        // create from non-rlmobject
        return RLMCreateObjectInRealmWithValue(realm, className, value, creationOptions & RLMCreationOptionsCreateOrUpdate);
    }

    if (link.isInvalidated) {
        @throw RLMException(@"Adding a deleted or invalidated object to a Realm is not permitted");
    }

    if (link->_realm == realm) {
        return link;
    }

    if (creationOptions & RLMCreationOptionsPromoteUnmanaged) {
        if (!link->_realm) {
            RLMAddObjectToRealm(link, realm, creationOptions & RLMCreationOptionsCreateOrUpdate);
            return link;
        }
        @throw RLMException(@"Can not add objects from a different Realm");
    }

    // copy from another realm or copy from unmanaged
    return RLMCreateObjectInRealmWithValue(realm, className, link, creationOptions & RLMCreationOptionsCreateOrUpdate);
}

// link getter/setter
static inline RLMObjectBase *RLMGetLink(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex, __unsafe_unretained NSString *const objectClassName) {
    RLMVerifyAttached(obj);

    if (obj->_row.is_null_link(colIndex)) {
        return nil;
    }
    NSUInteger index = obj->_row.get_link(colIndex);
    return RLMCreateObjectAccessor(obj->_realm, obj->_realm.schema[objectClassName], index);
}

static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex,
                               __unsafe_unretained RLMObjectBase *const val) {
    RLMVerifyInWriteTransaction(obj);

    if (!val) {
        obj->_row.nullify_link(colIndex);
    }
    else {
        // make sure it is the correct type
        RLMObjectSchema *valSchema = val->_objectSchema;
        RLMObjectSchema *objSchema = obj->_objectSchema;
        if (![[objSchema.properties[colIndex] objectClassName] isEqualToString:valSchema.className]) {
            @throw RLMException(@"Can't set object of type '%@' to property of type '%@'",
                                valSchema.className, [objSchema.properties[colIndex] objectClassName]);
        }
        RLMObjectBase *link = RLMGetLinkedObjectForValue(obj->_realm, valSchema.className, val, RLMCreationOptionsPromoteUnmanaged);
        obj->_row.set_link(colIndex, link->_row.get_index());
    }
}

// array getter/setter
static inline RLMArray *RLMGetArray(__unsafe_unretained RLMObjectBase *const obj,
                                    NSUInteger colIndex,
                                    __unsafe_unretained NSString *const objectClassName,
                                    __unsafe_unretained NSString *const propName) {
    RLMVerifyAttached(obj);

    realm::LinkViewRef linkView = obj->_row.get_linklist(colIndex);
    return [RLMArrayLinkView arrayWithObjectClassName:objectClassName
                                                 view:linkView
                                                realm:obj->_realm
                                                  key:propName
                                         parentSchema:obj->_objectSchema];
}

static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex,
                               __unsafe_unretained id<NSFastEnumeration> const array) {
    RLMVerifyInWriteTransaction(obj);

    realm::LinkViewRef linkView = obj->_row.get_linklist(colIndex);
    // remove all old
    // FIXME: make sure delete rules don't purge objects
    linkView->clear();
    for (RLMObjectBase *link in array) {
        RLMObjectBase * addedLink = RLMGetLinkedObjectForValue(obj->_realm, link->_objectSchema.className, link, RLMCreationOptionsPromoteUnmanaged);
        linkView->add(addedLink->_row.get_index());
    }
}

static inline NSNumber<RLMInt> *RLMGetIntObject(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex) {
    RLMVerifyAttached(obj);

    if (obj->_row.is_null(colIndex)) {
        return nil;
    }
    return @(obj->_row.get_int(colIndex));
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex,
                               __unsafe_unretained NSNumber<RLMInt> *const intObject) {
    RLMVerifyInWriteTransaction(obj);

    if (intObject) {
        obj->_row.set_int(colIndex, intObject.longLongValue);
    }
    else {
        obj->_row.set_null(colIndex);
    }
}
static inline void RLMSetValueUnique(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex, NSString *propName,
                                     __unsafe_unretained NSNumber<RLMInt> *const intObject) {
    RLMVerifyInWriteTransaction(obj);

    long long longLongValue = 0;
    size_t row;
    if (intObject) {
        longLongValue = intObject.longLongValue;
        row = obj->_row.get_table()->find_first_int(colIndex, longLongValue);
    }
    else {
        row = obj->_row.get_table()->find_first_null(colIndex);
    }

    if (row == obj->_row.get_index()) {
        return;
    }
    if (row != realm::not_found) {
        @throw RLMException(@"Can't set primary key property '%@' to existing value '%@'.", propName, intObject);
    }

    if (intObject) {
        obj->_row.set_int(colIndex, longLongValue);
    }
    else {
        obj->_row.set_null(colIndex);
    }
}

static inline NSNumber<RLMFloat> *RLMGetFloatObject(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex) {
    RLMVerifyAttached(obj);

    if (obj->_row.is_null(colIndex)) {
        return nil;
    }
    return @(obj->_row.get_float(colIndex));
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex,
                               __unsafe_unretained NSNumber<RLMFloat> *const floatObject) {
    RLMVerifyInWriteTransaction(obj);

    if (floatObject) {
        obj->_row.set_float(colIndex, floatObject.floatValue);
    }
    else {
        obj->_row.set_null(colIndex);
    }
}

static inline NSNumber<RLMDouble> *RLMGetDoubleObject(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex) {
    RLMVerifyAttached(obj);

    if (obj->_row.is_null(colIndex)) {
        return nil;
    }
    return @(obj->_row.get_double(colIndex));
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex,
                               __unsafe_unretained NSNumber<RLMDouble> *const doubleObject) {
    RLMVerifyInWriteTransaction(obj);

    if (doubleObject) {
        obj->_row.set_double(colIndex, doubleObject.doubleValue);
    }
    else {
        obj->_row.set_null(colIndex);
    }
}

static inline NSNumber<RLMBool> *RLMGetBoolObject(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex) {
    RLMVerifyAttached(obj);

    if (obj->_row.is_null(colIndex)) {
        return nil;
    }
    return @(obj->_row.get_bool(colIndex));
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger colIndex,
                               __unsafe_unretained NSNumber<RLMBool> *const boolObject) {
    RLMVerifyInWriteTransaction(obj);

    if (boolObject) {
        obj->_row.set_bool(colIndex, boolObject.boolValue);
    }
    else {
        obj->_row.set_null(colIndex);
    }
}

static inline RLMLinkingObjects *RLMGetLinkingObjects(__unsafe_unretained RLMObjectBase *const obj, __unsafe_unretained RLMProperty *const property) {
    RLMObjectSchema *objectSchema = obj->_realm.schema[property.objectClassName];
    RLMProperty *linkingProperty = objectSchema[property.linkOriginPropertyName];
    auto backlinkView = obj->_row.get_table()->get_backlink_view(obj->_row.get_index(), objectSchema.table, linkingProperty.column);
    realm::Results results(obj->_realm->_realm, {}, std::move(backlinkView));
    return [RLMLinkingObjects resultsWithObjectSchema:objectSchema results:std::move(results)];
}

// any getter/setter
static inline id RLMGetAnyProperty(__unsafe_unretained RLMObjectBase *const obj, NSUInteger col_ndx) {
    RLMVerifyAttached(obj);
    return RLMMixedToObjc(obj->_row.get_mixed(col_ndx));
}
static inline void RLMSetValue(__unsafe_unretained RLMObjectBase *const obj, NSUInteger, __unsafe_unretained id) {
    RLMVerifyInWriteTransaction(obj);
    @throw RLMException(@"Modifying Mixed properties is not supported");
}


// dynamic getter with column closure
static id RLMAccessorGetter(RLMProperty *prop, const char *type) {
    NSUInteger colIndex = prop.column;
    NSString *name = prop.name;
    NSString *objectClassName = prop.objectClassName;
    bool optional = prop.optional;
    switch (prop.type) {
        case RLMPropertyTypeInt:
            if (optional) {
                return ^(__unsafe_unretained RLMObjectBase *const obj) {
                    return RLMGetIntObject(obj, colIndex);
                };
            }
            switch (*type) {
                case 'c':
                    return ^(__unsafe_unretained RLMObjectBase *const obj) {
                        return RLMGetInt<char>(obj, colIndex);
                    };
                case 's':
                    return ^(__unsafe_unretained RLMObjectBase *const obj) {
                        return RLMGetInt<short>(obj, colIndex);
                    };
                case 'i':
                    return ^(__unsafe_unretained RLMObjectBase *const obj) {
                        return RLMGetInt<int>(obj, colIndex);
                    };
                case 'l':
                    return ^(__unsafe_unretained RLMObjectBase *const obj) {
                        return RLMGetInt<long>(obj, colIndex);
                    };
                case 'q':
                    return ^(__unsafe_unretained RLMObjectBase *const obj) {
                        return RLMGetInt<long long>(obj, colIndex);
                    };
                default:
                    @throw RLMException(@"Unexpected property type for Objective-C type code");
            }
        case RLMPropertyTypeFloat:
            if (optional) {
                return ^(__unsafe_unretained RLMObjectBase *const obj) {
                    return RLMGetFloatObject(obj, colIndex);
                };
            }
            return ^(__unsafe_unretained RLMObjectBase *const obj) {
                return RLMGetFloat(obj, colIndex);
            };
        case RLMPropertyTypeDouble:
            if (optional) {
                return ^(__unsafe_unretained RLMObjectBase *const obj) {
                    return RLMGetDoubleObject(obj, colIndex);
                };
            }
            return ^(__unsafe_unretained RLMObjectBase *const obj) {
                return RLMGetDouble(obj, colIndex);
            };
        case RLMPropertyTypeBool:
            if (optional) {
                return ^(__unsafe_unretained RLMObjectBase *const obj) {
                    return RLMGetBoolObject(obj, colIndex);
                };
            }
            return ^(__unsafe_unretained RLMObjectBase *const obj) {
                return RLMGetBool(obj, colIndex);
            };
        case RLMPropertyTypeString:
            return ^(__unsafe_unretained RLMObjectBase *const obj) {
                return RLMGetString(obj, colIndex);
            };
        case RLMPropertyTypeDate:
            return ^(__unsafe_unretained RLMObjectBase *const obj) {
                return RLMGetDate(obj, colIndex);
            };
        case RLMPropertyTypeData:
            return ^(__unsafe_unretained RLMObjectBase *const obj) {
                return RLMGetData(obj, colIndex);
            };
        case RLMPropertyTypeObject:
            return ^id(__unsafe_unretained RLMObjectBase *const obj) {
                return RLMGetLink(obj, colIndex, objectClassName);
            };
        case RLMPropertyTypeArray:
            return ^(__unsafe_unretained RLMObjectBase *const obj) {
                return RLMGetArray(obj, colIndex, objectClassName, name);
            };
        case RLMPropertyTypeAny:
            @throw RLMException(@"Cannot create accessor class for schema with Mixed properties");
        case RLMPropertyTypeLinkingObjects:
            return ^(__unsafe_unretained RLMObjectBase *const obj) {
                return RLMGetLinkingObjects(obj, prop);
            };
    }
}

template<typename Function>
static void RLMWrapSetter(__unsafe_unretained RLMObjectBase *const obj, __unsafe_unretained NSString *const name, Function&& f) {
    if (RLMObservationInfo *info = RLMGetObservationInfo(obj->_observationInfo, obj->_row.get_index(), obj->_objectSchema)) {
        info->willChange(name);
        f();
        info->didChange(name);
    }
    else {
        f();
    }
}

template<typename ArgType, typename StorageType=ArgType>
static id makeSetter(__unsafe_unretained RLMProperty *const prop) {
    NSUInteger colIndex = prop.column;
    NSString *name = prop.name;
    if (prop.isPrimary) {
        return ^(__unused RLMObjectBase *obj, __unused ArgType val) {
            @throw RLMException(@"Primary key can't be changed after an object is inserted.");
        };
    }
    return ^(__unsafe_unretained RLMObjectBase *const obj, ArgType val) {
        RLMWrapSetter(obj, name, [&] {
            RLMSetValue(obj, colIndex, static_cast<StorageType>(val));
        });
    };
}

// dynamic setter with column closure
static id RLMAccessorSetter(RLMProperty *prop, const char *type) {
    bool optional = prop.optional;
    switch (prop.type) {
        case RLMPropertyTypeInt:
            if (optional) {
                return makeSetter<NSNumber<RLMInt> *>(prop);
            }
            switch (*type) {
                case 'c': return makeSetter<char, long long>(prop);
                case 's': return makeSetter<short, long long>(prop);
                case 'i': return makeSetter<int, long long>(prop);
                case 'l': return makeSetter<long, long long>(prop);
                case 'q': return makeSetter<long long>(prop);
                default:
                    @throw RLMException(@"Unexpected property type for Objective-C type code");
            }
        case RLMPropertyTypeFloat:
            return optional ? makeSetter<NSNumber<RLMFloat> *>(prop) : makeSetter<float>(prop);
        case RLMPropertyTypeDouble:
            return optional ? makeSetter<NSNumber<RLMDouble> *>(prop) : makeSetter<double>(prop);
        case RLMPropertyTypeBool:
            return optional ? makeSetter<NSNumber<RLMBool> *>(prop) : makeSetter<BOOL>(prop);
        case RLMPropertyTypeString:         return makeSetter<NSString *>(prop);
        case RLMPropertyTypeDate:           return makeSetter<NSDate *>(prop);
        case RLMPropertyTypeData:           return makeSetter<NSData *>(prop);
        case RLMPropertyTypeObject:         return makeSetter<RLMObjectBase *>(prop);
        case RLMPropertyTypeArray:          return makeSetter<RLMArray *>(prop);
        case RLMPropertyTypeAny:            return makeSetter<id>(prop);
        case RLMPropertyTypeLinkingObjects: return nil;
    }
}

// call getter for superclass for property at colIndex
static id RLMSuperGet(RLMObjectBase *obj, NSString *propName) {
    typedef id (*getter_type)(RLMObjectBase *, SEL);
    RLMProperty *prop = obj->_objectSchema[propName];
    Class superClass = class_getSuperclass(obj.class);
    getter_type superGetter = (getter_type)[superClass instanceMethodForSelector:prop.getterSel];
    return superGetter(obj, prop.getterSel);
}

// call setter for superclass for property at colIndex
static void RLMSuperSet(RLMObjectBase *obj, NSString *propName, id val) {
    typedef void (*setter_type)(RLMObjectBase *, SEL, RLMArray *ar);
    RLMProperty *prop = obj->_objectSchema[propName];
    Class superClass = class_getSuperclass(obj.class);
    setter_type superSetter = (setter_type)[superClass instanceMethodForSelector:prop.setterSel];
    superSetter(obj, prop.setterSel, val);
}

// getter/setter for unmanaged object
static id RLMAccessorUnmanagedGetter(RLMProperty *prop, const char *) {
    // only override getters for RLMArray and linking objects properties
    if (prop.type == RLMPropertyTypeArray) {
        NSString *objectClassName = prop.objectClassName;
        NSString *propName = prop.name;

        return ^(RLMObjectBase *obj) {
            id val = RLMSuperGet(obj, propName);
            if (!val) {
                val = [[RLMArray alloc] initWithObjectClassName:objectClassName];
                RLMSuperSet(obj, propName, val);
            }
            return val;
        };
    }
    else if (prop.type == RLMPropertyTypeLinkingObjects) {
        return ^(RLMObjectBase *){
            return [RLMResults emptyDetachedResults];
        };
    }
    return nil;
}
static id RLMAccessorUnmanagedSetter(RLMProperty *prop, const char *) {
    if (prop.type != RLMPropertyTypeArray) {
        return nil;
    }

    NSString *propName = prop.name;
    NSString *objectClassName = prop.objectClassName;
    return ^(RLMObjectBase *obj, id<NSFastEnumeration> ar) {
        // make copy when setting (as is the case for all other variants)
        RLMArray *standaloneAr = [[RLMArray alloc] initWithObjectClassName:objectClassName];
        [standaloneAr addObjects:ar];
        RLMSuperSet(obj, propName, standaloneAr);
    };
}

// implement the class method className on accessors to return the className of the
// base object
void RLMReplaceClassNameMethod(Class accessorClass, NSString *className) {
    Class metaClass = object_getClass(accessorClass);
    IMP imp = imp_implementationWithBlock(^(Class){ return className; });
    class_addMethod(metaClass, @selector(className), imp, "@@:");
}

// implement the shared schema method
void RLMReplaceSharedSchemaMethod(Class accessorClass, RLMObjectSchema *schema) {
    Class metaClass = object_getClass(accessorClass);
    IMP imp = imp_implementationWithBlock(^(Class cls) {
        if (cls == accessorClass) {
            return schema;
        }

        // If we aren't being called directly on the class this was overriden
        // for, the class is either a subclass which we haven't initialized yet,
        // or it's a runtime-generated class which should use the parent's
        // schema. We check for the latter by checking if the immediate
        // descendent of the desired class is a class generated by us (there
        // may be further subclasses not generated by us for things like KVO).
        Class parent = class_getSuperclass(cls);
        while (parent != accessorClass) {
            cls = parent;
            parent = class_getSuperclass(cls);
        }

        static const char accessorClassPrefix[] = "RLMGenerated ";
        if (!strncmp(class_getName(cls), accessorClassPrefix, sizeof(accessorClassPrefix) - 1)) {
            return schema;
        }

        return [RLMSchema sharedSchemaForClass:cls];
    });
    class_addMethod(metaClass, @selector(sharedSchema), imp, "@@:");
}

static void addMethod(Class cls, __unsafe_unretained RLMProperty *const prop,
                      id (*getter)(RLMProperty *, const char *),
                      id (*setter)(RLMProperty *, const char *)) {
    SEL sel = prop.getterSel;
    auto getterMethod = class_getInstanceMethod(cls, sel);
    if (!getterMethod) {
        return;
    }

    const char *getterType = method_getTypeEncoding(getterMethod);
    if (id block = getter(prop, getterType)) {
        class_addMethod(cls, sel, imp_implementationWithBlock(block), getterType);
    }

    if (!(sel = prop.setterSel)) {
        return;
    }
    auto setterMethod = class_getInstanceMethod(cls, sel);
    if (!setterMethod) {
        return;
    }
    if (id block = setter(prop, getterType)) { // note: deliberate getter type as it's easier to grab the relevant type from
        class_addMethod(cls, sel, imp_implementationWithBlock(block), method_getTypeEncoding(setterMethod));
    }
}

static Class RLMCreateAccessorClass(Class objectClass,
                                    RLMObjectSchema *schema,
                                    const char *accessorClassName,
                                    id (*getterGetter)(RLMProperty *, const char *),
                                    id (*setterGetter)(RLMProperty *, const char *)) {
    REALM_ASSERT_DEBUG(RLMIsObjectOrSubclass(objectClass));

    // create and register proxy class which derives from object class
    Class accClass = objc_allocateClassPair(objectClass, accessorClassName, 0);
    if (!accClass) {
        // Class with that name already exists, so just return the pre-existing one
        // This should only happen for our standalone "accessors"
        return objc_lookUpClass(accessorClassName);
    }

    // override getters/setters for each propery
    for (RLMProperty *prop in schema.properties) {
        addMethod(accClass, prop, getterGetter, setterGetter);
    }
    for (RLMProperty *prop in schema.computedProperties) {
        addMethod(accClass, prop, getterGetter, setterGetter);
    }

    objc_registerClassPair(accClass);

    return accClass;
}

Class RLMManagedAccessorClassForObjectClass(Class objectClass, RLMObjectSchema *schema, const char *name) {
    return RLMCreateAccessorClass(objectClass, schema, name, RLMAccessorGetter, RLMAccessorSetter);
}

Class RLMUnmanagedAccessorClassForObjectClass(Class objectClass, RLMObjectSchema *schema) {
    return RLMCreateAccessorClass(objectClass, schema, [@"RLMGenerated Unmanaged " stringByAppendingString:schema.className].UTF8String,
                                  RLMAccessorUnmanagedGetter, RLMAccessorUnmanagedSetter);
}

void RLMDynamicValidatedSet(RLMObjectBase *obj, NSString *propName, id val) {
    RLMObjectSchema *schema = obj->_objectSchema;
    RLMProperty *prop = schema[propName];
    if (!prop) {
        @throw RLMException(@"Invalid property name `%@` for class `%@`.", propName, obj->_objectSchema.className);
    }
    if (prop.isPrimary) {
        @throw RLMException(@"Primary key can't be changed to '%@' after an object is inserted.", val);
    }
    if (!RLMIsObjectValidForProperty(val, prop)) {
        @throw RLMException(@"Invalid property value `%@` for property `%@` of class `%@`", val, propName, obj->_objectSchema.className);
    }

    RLMDynamicSet(obj, prop, RLMCoerceToNil(val), RLMCreationOptionsPromoteUnmanaged);
}

void RLMDynamicSet(__unsafe_unretained RLMObjectBase *const obj, __unsafe_unretained RLMProperty *const prop,
                   __unsafe_unretained id const val, RLMCreationOptions creationOptions) {
    NSUInteger col = prop.column;
    RLMWrapSetter(obj, prop.name, [&] {
        switch (prop.type) {
            case RLMPropertyTypeInt:
                if (prop.isPrimary) {
                    RLMSetValueUnique(obj, col, prop.name, (NSNumber<RLMInt> *)val);
                }
                else {
                    RLMSetValue(obj, col, (NSNumber<RLMInt> *)val);
                }
                break;
            case RLMPropertyTypeFloat:
                RLMSetValue(obj, col, (NSNumber<RLMFloat> *)val);
                break;
            case RLMPropertyTypeDouble:
                RLMSetValue(obj, col, (NSNumber<RLMDouble> *)val);
                break;
            case RLMPropertyTypeBool:
                RLMSetValue(obj, col, (NSNumber<RLMBool> *)val);
                break;
            case RLMPropertyTypeString:
                if (prop.isPrimary) {
                    RLMSetValueUnique(obj, col, prop.name, (NSString *)val);
                }
                else {
                    RLMSetValue(obj, col, (NSString *)val);
                }
                break;
            case RLMPropertyTypeDate:
                RLMSetValue(obj, col, (NSDate *)val);
                break;
            case RLMPropertyTypeData:
                RLMSetValue(obj, col, (NSData *)val);
                break;
            case RLMPropertyTypeObject: {
                if (!val || val == NSNull.null) {
                    RLMSetValue(obj, col, (RLMObjectBase *)nil);
                }
                else {
                    RLMSetValue(obj, col, RLMGetLinkedObjectForValue(obj->_realm, prop.objectClassName, val, creationOptions));
                }
                break;
            }
            case RLMPropertyTypeArray:
                if (!val || val == NSNull.null) {
                    RLMSetValue(obj, col, (id<NSFastEnumeration>)nil);
                }
                else {
                    id<NSFastEnumeration> rawLinks = val;
                    NSMutableArray *links = [NSMutableArray array];
                    for (id rawLink in rawLinks) {
                        [links addObject:RLMGetLinkedObjectForValue(obj->_realm, prop.objectClassName, rawLink, creationOptions)];
                    }
                    RLMSetValue(obj, col, links);
                }
                break;
            case RLMPropertyTypeAny:
                RLMSetValue(obj, col, val);
                break;
            case RLMPropertyTypeLinkingObjects:
                @throw RLMException(@"Linking objects properties are read-only");
        }
    });
}

RLMProperty *RLMValidatedGetProperty(__unsafe_unretained RLMObjectBase *const obj, __unsafe_unretained NSString *const propName) {
    RLMProperty *prop = obj->_objectSchema[propName];
    if (!prop) {
        @throw RLMException(@"Invalid property name `%@` for class `%@`.", propName, obj->_objectSchema.className);
    }
    return prop;
}

id RLMDynamicGet(__unsafe_unretained RLMObjectBase *obj, __unsafe_unretained RLMProperty *prop) {
    NSUInteger col = prop.column;
    switch (prop.type) {
        case RLMPropertyTypeInt:            return RLMGetIntObject(obj, col);
        case RLMPropertyTypeFloat:          return RLMGetFloatObject(obj, col);
        case RLMPropertyTypeDouble:         return RLMGetDoubleObject(obj, col);
        case RLMPropertyTypeBool:           return RLMGetBoolObject(obj, col);
        case RLMPropertyTypeString:         return RLMGetString(obj, col);
        case RLMPropertyTypeDate:           return RLMGetDate(obj, col);
        case RLMPropertyTypeData:           return RLMGetData(obj, col);
        case RLMPropertyTypeObject:         return RLMGetLink(obj, col, prop.objectClassName);
        case RLMPropertyTypeArray:          return RLMGetArray(obj, col, prop.objectClassName, prop.name);
        case RLMPropertyTypeAny:            return RLMGetAnyProperty(obj, col);
        case RLMPropertyTypeLinkingObjects: return RLMGetLinkingObjects(obj, prop);
    }
}
